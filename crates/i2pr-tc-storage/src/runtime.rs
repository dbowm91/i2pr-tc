//! Single-owner composition of peer block scheduling, verified storage, and service progress.
use crate::executor::{ResultSlot, StorageExecutor};
use crate::{Cancellation, PersistentTorrentService};
use i2pr_tc_core::{
    magnet, metainfo,
    metainfo::InfoHashV1,
    service::{ServiceError, TorrentCommand, TorrentId, TorrentService, TorrentStatus},
    state::{BlockRequest, PieceMap, PieceStatus, ScheduleError},
    wire::{Handshake, Message, PeerEvent, PeerWireSession},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

const MAX_RUNTIME_PIECES: usize = 4_000_000;
const MAX_BLOCK_LENGTH: u32 = 16 * 1024;
const MAX_BUFFERED_BLOCK_BYTES: usize = 64 * 1024 * 1024;

/// Scheduler and assembly state for exactly one torrent.
///
/// Each torrent owns its own lock. The catalog map below is held only long
/// enough to clone an `Arc` to one of these, so one torrent's disk operation
/// can never block another's state transitions.
struct TorrentRuntimeState {
    piece_length: u32,
    total_length: u64,
    pieces: PieceMap,
    requests: BTreeMap<(u32, u32, [u8; 32]), BlockRequest>,
    blocks: BTreeMap<u32, Vec<(u32, Vec<u8>)>>,
    buffered_bytes: usize,
    peers: BTreeMap<[u8; 32], PeerWireSession>,
    /// Identity of the state generation. A piece persistence that started under
    /// an older generation is stale on completion and must not resurrect or
    /// double count progress. Values come from the runtime-wide counter, so a
    /// re-registered torrent never reuses a live generation.
    generation: u64,
    /// Pieces whose assembled bytes have already left this state owner and are
    /// being written by the storage layer.
    persisting: BTreeSet<u32>,
}

/// One assembled piece whose bytes have left the torrent state owner and are
/// reserved for one storage write.
struct PendingPiece {
    piece: u32,
    generation: u64,
    bytes: Vec<u8>,
}

/// The identity of a reservation, kept without the assembled bytes so a
/// refused submission can still give the piece back.
#[derive(Clone, Copy)]
struct Reservation {
    piece: u32,
    generation: u64,
}

impl PendingPiece {
    fn reservation(&self) -> Reservation {
        Reservation {
            piece: self.piece,
            generation: self.generation,
        }
    }
}

/// Outcome of one piece persistence.
///
/// The synchronous entry points collapse everything except
/// [`Verified`](Self::Verified) into a `bool` or an error. The async surface
/// keeps the distinctions so a caller can tell a clean completion from a write
/// that lost its reservation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PieceCompletion {
    /// The block did not complete its piece; nothing was persisted.
    Incomplete,
    /// The piece was stored, verified, and is now reflected in the snapshot.
    Verified,
    /// The storage work failed and the piece is available for a retry.
    Failed,
    /// The write finished after its reservation was invalidated, so no
    /// progress was recorded.
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerMessageOutcome {
    pub event: PeerEvent,
    pub cancellations: Vec<Message>,
    pub completed_piece: Option<bool>,
}

/// Owns scheduler and storage transitions for one persistent torrent catalog.
/// Construct with [`TorrentRuntime::open`] so saved progress is rechecked first.
pub struct TorrentRuntime {
    service: Arc<PersistentTorrentService>,
    /// Torrent catalog. The lock guards the map only; each value carries its
    /// own bounded lock so state transitions for different torrents proceed
    /// independently.
    torrents: Mutex<BTreeMap<TorrentId, Arc<Mutex<TorrentRuntimeState>>>>,
    /// Runtime-wide generation counter. Starts at one so zero is never a live
    /// generation and a default-constructed state cannot look authoritative.
    generation: AtomicU64,
    max_torrents: usize,
    max_inflight_per_torrent: usize,
    max_inflight_per_peer: usize,
}

impl TorrentRuntime {
    pub fn open(
        root: impl AsRef<Path>,
        cancellation: &Cancellation,
        max_torrents: usize,
        max_inflight_per_torrent: usize,
        max_inflight_per_peer: usize,
    ) -> Result<Self, ServiceError> {
        if max_torrents == 0
            || max_inflight_per_torrent == 0
            || max_inflight_per_torrent > 4096
            || max_inflight_per_peer == 0
            || max_inflight_per_peer > max_inflight_per_torrent
        {
            return Err(ServiceError::InvalidInput);
        }
        if cancellation.is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        let service = PersistentTorrentService::open(root).map_err(|_| ServiceError::Storage)?;
        let snapshots = service.list()?;
        if snapshots.len() > max_torrents {
            return Err(ServiceError::InvalidInput);
        }
        for snapshot in snapshots {
            if service.get_metainfo(snapshot.id)?.is_some() {
                service.verify_and_recover(snapshot.id, cancellation)?;
            }
        }
        let snapshots = service.list()?;
        let generation = AtomicU64::new(1);
        let mut torrents = BTreeMap::new();
        for snapshot in snapshots {
            let meta = service.get_metainfo(snapshot.id)?;
            let piece_count = meta.as_ref().map_or(0, |meta| meta.piece_hashes.len());
            let mut pieces = PieceMap::new(
                piece_count,
                MAX_RUNTIME_PIECES,
                max_inflight_per_torrent,
                max_inflight_per_peer,
            )
            .map_err(map_schedule)?;
            if piece_count > 0 && snapshot.verified_pieces.len() != piece_count {
                return Err(ServiceError::Storage);
            }
            for (index, verified) in snapshot.verified_pieces.iter().enumerate() {
                if *verified {
                    pieces.mark_verified(index as u32).map_err(map_schedule)?;
                }
            }
            let state = TorrentRuntimeState {
                piece_length: meta.as_ref().map_or(0, |meta| meta.piece_length),
                total_length: meta.as_ref().map_or(0, |meta| meta.total_length),
                pieces,
                requests: BTreeMap::new(),
                blocks: BTreeMap::new(),
                buffered_bytes: 0,
                peers: BTreeMap::new(),
                generation: next_generation(&generation),
                persisting: BTreeSet::new(),
            };
            torrents.insert(snapshot.id, Arc::new(Mutex::new(state)));
        }
        Ok(Self {
            service: Arc::new(service),
            torrents: Mutex::new(torrents),
            generation,
            max_torrents,
            max_inflight_per_torrent,
            max_inflight_per_peer,
        })
    }

    pub fn service(&self) -> &PersistentTorrentService {
        &self.service
    }

    /// Locate one torrent's state owner. The catalog lock is released before
    /// the caller takes the per-torrent lock, so nothing here can be held
    /// across a caller's own work.
    fn torrent(&self, id: TorrentId) -> Result<Arc<Mutex<TorrentRuntimeState>>, ServiceError> {
        self.torrents
            .lock()
            .map_err(|_| ServiceError::Storage)?
            .get(&id)
            .cloned()
            .ok_or(ServiceError::NotFound)
    }

    /// Route lifecycle commands through the runtime so stop, verify, and remove also
    /// invalidate scheduler ownership and buffered blocks.
    pub fn command(&self, command: TorrentCommand) -> Result<(), ServiceError> {
        self.service.command(command.clone())?;
        match command {
            TorrentCommand::Stop(id) | TorrentCommand::Verify(id) => self.register(id),
            TorrentCommand::Remove { id, .. } => {
                self.torrents
                    .lock()
                    .map_err(|_| ServiceError::Storage)?
                    .remove(&id);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub fn add_metainfo(&self, bytes: &[u8]) -> Result<TorrentId, ServiceError> {
        let meta =
            metainfo::parse(bytes, Default::default()).map_err(|_| ServiceError::InvalidInput)?;
        self.ensure_capacity(meta.info_hash.0)?;
        let id = self.service.add_metainfo(bytes)?;
        self.register(id)?;
        Ok(id)
    }

    pub fn add_magnet(&self, uri: &str) -> Result<TorrentId, ServiceError> {
        let magnet =
            magnet::parse(uri, Default::default()).map_err(|_| ServiceError::InvalidInput)?;
        self.ensure_capacity(magnet.info_hash.0)?;
        let id = self.service.add_magnet(uri)?;
        self.register(id)?;
        Ok(id)
    }

    /// Call after metadata for a magnet has been received and promoted in the service.
    pub fn refresh_metainfo(&self, id: TorrentId) -> Result<(), ServiceError> {
        let meta = self
            .service
            .get_metainfo(id)?
            .ok_or(ServiceError::Unsupported)?;
        let snapshot = self.service.get(id)?;
        let piece_count = meta.piece_hashes.len();
        let mut pieces = PieceMap::new(
            piece_count,
            MAX_RUNTIME_PIECES,
            self.max_inflight_per_torrent,
            self.max_inflight_per_peer,
        )
        .map_err(map_schedule)?;
        for (index, verified) in snapshot.verified_pieces.iter().enumerate() {
            if *verified {
                pieces.mark_verified(index as u32).map_err(map_schedule)?;
            }
        }
        let owner = self.torrent(id)?;
        let mut guard = owner.lock().map_err(|_| ServiceError::Storage)?;
        let state = &mut *guard;
        // Metadata promotion retains established transport sessions, but resets all
        // availability and scheduling state because it had no piece geometry before.
        if !state.requests.is_empty() || !state.blocks.is_empty() {
            return Err(ServiceError::Conflict);
        }
        for session in state.peers.values_mut() {
            session
                .update_metadata(meta.info_hash.0, meta.piece_length, meta.total_length)
                .map_err(|_| ServiceError::Conflict)?;
        }
        state.piece_length = meta.piece_length;
        state.total_length = meta.total_length;
        state.pieces = pieces;
        state.requests.clear();
        state.blocks.clear();
        state.buffered_bytes = 0;
        Ok(())
    }

    pub fn set_peer_availability(
        &self,
        id: TorrentId,
        peer: [u8; 32],
        pieces: &[bool],
    ) -> Result<(), ServiceError> {
        let owner = self.torrent(id)?;
        let mut guard = owner.lock().map_err(|_| ServiceError::Storage)?;
        let state = &mut *guard;
        state
            .pieces
            .set_availability(peer, pieces)
            .map_err(map_schedule)
    }

    pub fn accept_peer_handshake(
        &self,
        id: TorrentId,
        peer: [u8; 32],
        bytes: &[u8],
    ) -> Result<Handshake, ServiceError> {
        let snapshot = self.service.get(id)?;
        let meta = self.service.get_metainfo(id)?;
        let info_hash = meta
            .as_ref()
            .map_or(snapshot.info_hash, |meta| meta.info_hash.0);
        let piece_length = meta.as_ref().map_or(1, |meta| meta.piece_length);
        let total_length = meta.as_ref().map_or(0, |meta| meta.total_length);
        let piece_count = meta.as_ref().map_or(0, |meta| meta.piece_hashes.len());
        let mut session = PeerWireSession::new(
            info_hash,
            piece_length,
            total_length,
            MAX_BLOCK_LENGTH,
            self.max_inflight_per_peer,
        )
        .map_err(|_| ServiceError::InvalidInput)?;
        let handshake = session
            .accept_handshake(bytes)
            .map_err(|_| ServiceError::InvalidInput)?;
        let owner = self.torrent(id)?;
        let mut guard = owner.lock().map_err(|_| ServiceError::Storage)?;
        let state = &mut *guard;
        if state.peers.contains_key(&peer) {
            return Err(ServiceError::Conflict);
        }
        state
            .pieces
            .set_availability(peer, &vec![false; piece_count])
            .map_err(map_schedule)?;
        state.peers.insert(peer, session);
        Ok(handshake)
    }

    pub fn process_peer_message(
        &self,
        id: TorrentId,
        peer: [u8; 32],
        message: Message,
    ) -> Result<PeerMessageOutcome, ServiceError> {
        self.process_peer_message_inner(id, peer, message, None)
    }

    pub fn process_peer_message_cancellable(
        &self,
        id: TorrentId,
        peer: [u8; 32],
        message: Message,
        cancellation: &Cancellation,
    ) -> Result<PeerMessageOutcome, ServiceError> {
        self.process_peer_message_inner(id, peer, message, Some(cancellation))
    }

    fn process_peer_message_inner(
        &self,
        id: TorrentId,
        peer: [u8; 32],
        message: Message,
        cancellation: Option<&Cancellation>,
    ) -> Result<PeerMessageOutcome, ServiceError> {
        let (event, cancellations, incoming_block) = {
            let owner = self.torrent(id)?;
            let mut guard = owner.lock().map_err(|_| ServiceError::Storage)?;
            let state = &mut *guard;
            let session = state.peers.get_mut(&peer).ok_or(ServiceError::NotFound)?;
            let event = session
                .on_message(message)
                .map_err(|_| ServiceError::InvalidInput)?;
            let mut cancellations = Vec::new();
            if event == PeerEvent::Choked {
                cancellations = session.clear_pending_downloads();
                state.pieces.cancel_peer_requests(peer);
                state.requests.retain(|(_, _, owner), _| *owner != peer);
            }
            match &event {
                PeerEvent::Have(_) | PeerEvent::Bitfield(_) => {
                    state
                        .pieces
                        .set_availability(peer, session.remote_pieces())
                        .map_err(map_schedule)?;
                }
                _ => {}
            }
            let incoming = if let PeerEvent::DownloadPiece {
                index,
                begin,
                block,
            } = &event
            {
                Some((
                    state
                        .requests
                        .get(&(*index, *begin, peer))
                        .cloned()
                        .ok_or(ServiceError::Conflict)?,
                    block.clone(),
                ))
            } else {
                None
            };
            (event, cancellations, incoming)
        };
        let completed_piece = if let Some((request, bytes)) = incoming_block {
            Some(self.receive_block_inner(id, &request, &bytes, cancellation)?)
        } else {
            None
        };
        Ok(PeerMessageOutcome {
            event,
            cancellations,
            completed_piece,
        })
    }

    pub fn disconnect_peer(&self, id: TorrentId, peer: [u8; 32]) -> Result<(), ServiceError> {
        let owner = self.torrent(id)?;
        let mut guard = owner.lock().map_err(|_| ServiceError::Storage)?;
        let state = &mut *guard;
        state.pieces.disconnect_peer(peer);
        state.peers.remove(&peer);
        state.requests.retain(|(_, _, owner), _| *owner != peer);
        let requested: std::collections::BTreeSet<_> =
            state.requests.keys().map(|(piece, _, _)| *piece).collect();
        let retained: BTreeMap<_, _> = state
            .blocks
            .iter()
            .filter(|(piece, _)| requested.contains(piece))
            .map(|(piece, blocks)| (*piece, blocks.clone()))
            .collect();
        state.buffered_bytes = retained
            .values()
            .flatten()
            .map(|(_, block)| block.len())
            .sum();
        state.blocks = retained;
        Ok(())
    }

    pub fn choose_piece(&self, id: TorrentId, peer: [u8; 32]) -> Result<Option<u32>, ServiceError> {
        let owner = self.torrent(id)?;
        let mut guard = owner.lock().map_err(|_| ServiceError::Storage)?;
        let state = &mut *guard;
        Ok(state.pieces.choose_piece(peer))
    }

    pub fn request_block(
        &self,
        id: TorrentId,
        peer: [u8; 32],
        piece: u32,
        begin: u32,
        length: u32,
    ) -> Result<BlockRequest, ServiceError> {
        let snapshot = self.service.get(id)?;
        if !matches!(
            snapshot.status,
            TorrentStatus::Running | TorrentStatus::Starting
        ) {
            return Err(ServiceError::Conflict);
        }
        let owner = self.torrent(id)?;
        let mut guard = owner.lock().map_err(|_| ServiceError::Storage)?;
        let state = &mut *guard;
        let offset = (piece as u64)
            .checked_mul(state.piece_length as u64)
            .ok_or(ServiceError::InvalidInput)?;
        let piece_size = if state.total_length == 0 || offset >= state.total_length {
            return Err(ServiceError::InvalidInput);
        } else {
            (state.total_length - offset).min(state.piece_length as u64) as u32
        };
        let end = begin
            .checked_add(length)
            .ok_or(ServiceError::InvalidInput)?;
        if length == 0 || length > MAX_BLOCK_LENGTH || end > piece_size {
            return Err(ServiceError::InvalidInput);
        }
        if state.requests.values().any(|active| {
            active.piece == piece
                && begin < active.begin.saturating_add(active.length)
                && active.begin < end
        }) {
            return Err(ServiceError::Conflict);
        }
        if state.blocks.get(&piece).is_some_and(|blocks| {
            blocks.iter().any(|(block_begin, block)| {
                begin < (*block_begin).saturating_add(block.len() as u32) && *block_begin < end
            })
        }) {
            return Err(ServiceError::Conflict);
        }
        let request = state
            .pieces
            .request_block(peer, piece, begin, length, piece_size, MAX_BLOCK_LENGTH)
            .map_err(map_schedule)?;
        if let Some(session) = state.peers.get_mut(&peer) {
            if session.request_block(piece, begin, length).is_err() {
                state.pieces.complete_block(&request);
                return Err(ServiceError::Conflict);
            }
        }
        state.requests.insert((piece, begin, peer), request.clone());
        Ok(request)
    }

    /// Accepts only a block matching an outstanding request. Returns true once its complete
    /// piece has been assembled, hash-checked, stored, and reflected in the service snapshot.
    pub fn receive_block(
        &self,
        id: TorrentId,
        request: &BlockRequest,
        bytes: &[u8],
    ) -> Result<bool, ServiceError> {
        self.receive_block_inner(id, request, bytes, None)
    }

    pub fn receive_block_cancellable(
        &self,
        id: TorrentId,
        request: &BlockRequest,
        bytes: &[u8],
        cancellation: &Cancellation,
    ) -> Result<bool, ServiceError> {
        self.receive_block_inner(id, request, bytes, Some(cancellation))
    }

    /// Validate the block, assemble it under the torrent lock, and move a
    /// completed piece out of the state owner as an explicit reservation.
    ///
    /// Every filesystem operation happens after this returns, so no torrent
    /// lock and no catalog lock is held across storage work.
    fn reserve_piece(
        &self,
        id: TorrentId,
        request: &BlockRequest,
        bytes: &[u8],
    ) -> Result<Option<PendingPiece>, ServiceError> {
        let owner = self.torrent(id)?;
        let mut guard = owner.lock().map_err(|_| ServiceError::Storage)?;
        let state = &mut *guard;
        let key = (request.piece, request.begin, request.peer);
        if state.requests.get(&key) != Some(request) {
            return Err(ServiceError::Conflict);
        }
        if state.buffered_bytes.saturating_add(bytes.len()) > MAX_BUFFERED_BLOCK_BYTES {
            return Err(ServiceError::Conflict);
        }
        let blocks = state.blocks.entry(request.piece).or_default();
        blocks.push((request.begin, bytes.to_vec()));
        state.buffered_bytes += bytes.len();
        blocks.sort_by_key(|(begin, _)| *begin);
        state.requests.remove(&key);
        state.pieces.complete_block(request);

        let piece_size = (state.total_length - (request.piece as u64 * state.piece_length as u64))
            .min(state.piece_length as u64) as usize;
        let mut assembled = Vec::with_capacity(piece_size);
        for (begin, block) in blocks.iter() {
            if *begin as usize != assembled.len() {
                return Ok(None);
            }
            assembled.extend_from_slice(block);
        }
        if assembled.len() != piece_size {
            return Ok(None);
        }
        let piece = request.piece;
        if let Some(blocks) = state.blocks.remove(&piece) {
            state.buffered_bytes -= blocks.iter().map(|(_, block)| block.len()).sum::<usize>();
        }
        state.requests.retain(|(p, _, _), _| *p != piece);
        if !state.persisting.insert(piece) {
            // A write for this piece is already in flight under this generation.
            return Err(ServiceError::Conflict);
        }
        Ok(Some(PendingPiece {
            piece,
            generation: state.generation,
            bytes: assembled,
        }))
    }

    /// Reconcile a finished storage write with the torrent state.
    ///
    /// The write is only allowed to publish progress while its reservation is
    /// still the authority. A stop, remove, or recheck mints a new generation
    /// and drops the reservation, so the late completion reports
    /// [`PieceCompletion::Stale`] instead of resurrecting state or counting the
    /// same bytes twice.
    fn commit_piece(
        &self,
        id: TorrentId,
        reservation: &Reservation,
        result: &Result<(), ServiceError>,
    ) -> PieceCompletion {
        // A removed torrent has no state owner left, so the write is stale.
        let Ok(owner) = self.torrent(id) else {
            return PieceCompletion::Stale;
        };
        let mut guard = match owner.lock() {
            Ok(guard) => guard,
            Err(_) => return PieceCompletion::Stale,
        };
        let state = &mut *guard;
        if state.generation != reservation.generation {
            return PieceCompletion::Stale;
        }
        if !state.persisting.remove(&reservation.piece) {
            return PieceCompletion::Stale;
        }
        match result {
            Ok(()) => match state.pieces.mark_verified(reservation.piece) {
                Ok(()) => PieceCompletion::Verified,
                Err(_) => PieceCompletion::Failed,
            },
            Err(_) => {
                let _ = state.pieces.reset_piece(reservation.piece);
                PieceCompletion::Failed
            }
        }
    }

    /// Give a reserved piece back after the write could not even be submitted.
    fn abandon_reservation(&self, id: TorrentId, reservation: &Reservation) {
        let Ok(owner) = self.torrent(id) else {
            return;
        };
        let Ok(mut guard) = owner.lock() else {
            return;
        };
        let state = &mut *guard;
        if state.generation == reservation.generation {
            state.persisting.remove(&reservation.piece);
            let _ = state.pieces.reset_piece(reservation.piece);
        }
    }

    fn store_pending(
        &self,
        id: TorrentId,
        pending: &PendingPiece,
        cancellation: Option<&Cancellation>,
    ) -> Result<(), ServiceError> {
        match cancellation {
            Some(token) => self.service.store_piece_with_cancellation(
                id,
                pending.piece,
                &pending.bytes,
                Some(token),
            ),
            None => self
                .service
                .store_verified_piece(id, pending.piece, &pending.bytes),
        }
    }

    fn receive_block_inner(
        &self,
        id: TorrentId,
        request: &BlockRequest,
        bytes: &[u8],
        cancellation: Option<&Cancellation>,
    ) -> Result<bool, ServiceError> {
        if cancellation.is_some_and(Cancellation::is_cancelled) {
            self.release_request(id, request)?;
            return Err(ServiceError::Cancelled);
        }
        if bytes.len() != request.length as usize {
            return Err(ServiceError::InvalidInput);
        }
        let Some(pending) = self.reserve_piece(id, request, bytes)? else {
            return Ok(false);
        };
        let reservation = pending.reservation();
        let result = self.store_pending(id, &pending, cancellation);
        match self.commit_piece(id, &reservation, &result) {
            PieceCompletion::Verified => Ok(true),
            // A stale completion reports "not completed" rather than reviving
            // progress for a torrent that has since been stopped or removed.
            PieceCompletion::Incomplete | PieceCompletion::Stale => Ok(false),
            PieceCompletion::Failed => Err(result.err().unwrap_or(ServiceError::Storage)),
        }
    }

    /// Async block receipt that keeps blocking piece writes off the caller's
    /// task.
    ///
    /// The blocking write runs on the injected [`StorageExecutor`], whose
    /// bounded queue is the backpressure authority: a full queue returns
    /// [`crate::StorageError::Backpressure`] without ever queueing without
    /// limit.
    pub async fn receive_block_offloaded(
        &self,
        id: TorrentId,
        request: &BlockRequest,
        bytes: &[u8],
        cancellation: &Cancellation,
        executor: &dyn StorageExecutor,
    ) -> Result<bool, ServiceError> {
        if cancellation.is_cancelled() {
            self.release_request(id, request)?;
            return Err(ServiceError::Cancelled);
        }
        if bytes.len() != request.length as usize {
            return Err(ServiceError::InvalidInput);
        }
        let Some(pending) = self.reserve_piece(id, request, bytes)? else {
            return Ok(false);
        };
        let reservation = pending.reservation();
        let token = cancellation.clone();
        let result = self
            .submit(id, pending, Some(token), executor)
            .await?
            .unwrap_or(Err(ServiceError::Storage));
        match self.commit_piece(id, &reservation, &result) {
            PieceCompletion::Verified => Ok(true),
            PieceCompletion::Incomplete | PieceCompletion::Stale => Ok(false),
            PieceCompletion::Failed => Err(result.err().unwrap_or(ServiceError::Storage)),
        }
    }

    /// Hand one reserved piece to the executor and await its result.
    ///
    /// `Ok(None)` means the submission was refused, in which case the
    /// reservation is released and no storage work was started.
    async fn submit(
        &self,
        id: TorrentId,
        pending: PendingPiece,
        cancellation: Option<Cancellation>,
        executor: &dyn StorageExecutor,
    ) -> Result<Option<Result<(), ServiceError>>, ServiceError> {
        let slot = ResultSlot::new();
        let service = Arc::clone(&self.service);
        let job_slot = Arc::clone(&slot);
        let reservation = pending.reservation();
        executor
            .spawn_blocking(Box::new(move || {
                let result = match cancellation {
                    Some(token) => service.store_piece_with_cancellation(
                        id,
                        pending.piece,
                        &pending.bytes,
                        Some(&token),
                    ),
                    None => service.store_verified_piece(id, pending.piece, &pending.bytes),
                };
                job_slot.complete(result);
            }))
            .map_err(|_| {
                // The job was never started, so the reservation goes back.
                self.abandon_reservation(id, &reservation);
                ServiceError::Storage
            })?;
        Ok(Some(slot.waiter().await))
    }

    fn release_request(&self, id: TorrentId, request: &BlockRequest) -> Result<(), ServiceError> {
        let owner = self.torrent(id)?;
        let mut guard = owner.lock().map_err(|_| ServiceError::Storage)?;
        let state = &mut *guard;
        let key = (request.piece, request.begin, request.peer);
        if state.requests.get(&key) == Some(request) {
            let pending: Vec<_> = state
                .requests
                .iter()
                .filter(|((piece, _, _), _)| *piece == request.piece)
                .map(|(key, request)| (*key, request.clone()))
                .collect();
            for ((piece, begin, peer), block_request) in pending {
                state.requests.remove(&(piece, begin, peer));
                if let Some(session) = state.peers.get_mut(&peer) {
                    session.cancel_block(piece, begin, block_request.length);
                }
            }
            state
                .pieces
                .reset_piece(request.piece)
                .map_err(map_schedule)?;
            if let Some(blocks) = state.blocks.remove(&request.piece) {
                state.buffered_bytes -= blocks.iter().map(|(_, block)| block.len()).sum::<usize>();
            }
        }
        Ok(())
    }

    pub fn piece_status(
        &self,
        id: TorrentId,
        piece: u32,
    ) -> Result<Option<PieceStatus>, ServiceError> {
        let owner = self.torrent(id)?;
        let mut guard = owner.lock().map_err(|_| ServiceError::Storage)?;
        let state = &mut *guard;
        Ok(state.pieces.status(piece))
    }

    pub fn read_verified_block(
        &self,
        id: TorrentId,
        piece: u32,
        begin: u32,
        length: u32,
    ) -> Result<Vec<u8>, ServiceError> {
        if length == 0 || length > MAX_BLOCK_LENGTH {
            return Err(ServiceError::InvalidInput);
        }
        let snapshot = self.service.get(id)?;
        if snapshot.verified_pieces.get(piece as usize) != Some(&true) {
            return Err(ServiceError::Conflict);
        }
        let meta = self
            .service
            .get_metainfo(id)?
            .ok_or(ServiceError::Unsupported)?;
        // Share the per-torrent payload handle so reads exclude in-flight
        // writes for the same payload root instead of racing them.
        let storage = self
            .service
            .payload_storage(id)
            .map_err(|_| ServiceError::Storage)?;
        storage
            .read_block(
                &meta.files,
                meta.piece_length,
                piece,
                begin,
                length as usize,
            )
            .map_err(|_| ServiceError::Storage)
    }

    fn ensure_capacity(&self, hash: [u8; 20]) -> Result<(), ServiceError> {
        if self.service.list()?.len() >= self.max_torrents
            && self.service.find_by_hash(InfoHashV1(hash))?.is_none()
        {
            return Err(ServiceError::InvalidInput);
        }
        Ok(())
    }

    fn register(&self, id: TorrentId) -> Result<(), ServiceError> {
        let meta = self.service.get_metainfo(id)?;
        let snapshot = self.service.get(id)?;
        let piece_count = meta.as_ref().map_or(0, |meta| meta.piece_hashes.len());
        let mut pieces = PieceMap::new(
            piece_count,
            MAX_RUNTIME_PIECES,
            self.max_inflight_per_torrent,
            self.max_inflight_per_peer,
        )
        .map_err(map_schedule)?;
        for (index, verified) in snapshot.verified_pieces.iter().enumerate() {
            if *verified {
                pieces.mark_verified(index as u32).map_err(map_schedule)?;
            }
        }
        // Re-registration mints a fresh generation, which is what makes every
        // persistence already in flight for this torrent report as stale.
        let state = TorrentRuntimeState {
            piece_length: meta.as_ref().map_or(0, |meta| meta.piece_length),
            total_length: meta.as_ref().map_or(0, |meta| meta.total_length),
            pieces,
            requests: BTreeMap::new(),
            blocks: BTreeMap::new(),
            buffered_bytes: 0,
            peers: BTreeMap::new(),
            generation: next_generation(&self.generation),
            persisting: BTreeSet::new(),
        };
        self.torrents
            .lock()
            .map_err(|_| ServiceError::Storage)?
            .insert(id, Arc::new(Mutex::new(state)));
        Ok(())
    }
}

/// Mint the next never-reused persistence generation.
fn next_generation(counter: &AtomicU64) -> u64 {
    counter.fetch_add(1, Ordering::Relaxed)
}

fn map_schedule(error: ScheduleError) -> ServiceError {
    match error {
        ScheduleError::Bounds | ScheduleError::InvalidBlock => ServiceError::InvalidInput,
        ScheduleError::InFlightLimit => ServiceError::Conflict,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BlockingStoragePool;
    use i2pr_tc_core::{
        service::{TorrentCommand, TorrentStatus},
        state::PieceStatus,
    };
    use sha1::{Digest, Sha1};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn root() -> std::path::PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "i2pr-tc-runtime-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn metainfo() -> Vec<u8> {
        metainfo_named("x")
    }

    /// Single-piece metainfo for one four-byte payload, named so two distinct
    /// torrents can be registered in one runtime.
    fn metainfo_named(name: &str) -> Vec<u8> {
        let mut bytes = format!(
            "d4:infod6:lengthi4e4:name{}:{name}12:piece lengthi4e6:pieces20:",
            name.len()
        )
        .into_bytes();
        bytes.extend(Sha1::digest(b"data"));
        bytes.extend_from_slice(b"ee");
        bytes
    }

    #[test]
    fn runtime_assembles_requested_blocks_and_rechecks_on_open() {
        let root = root();
        let runtime = TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap();
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime
            .service()
            .command(TorrentCommand::Start(id))
            .unwrap();
        let peer = [7; 32];
        runtime.set_peer_availability(id, peer, &[true]).unwrap();
        assert_eq!(runtime.choose_piece(id, peer).unwrap(), Some(0));
        let second = runtime.request_block(id, peer, 0, 2, 2).unwrap();
        assert_eq!(
            runtime.request_block(id, [8; 32], 0, 3, 1),
            Err(ServiceError::Conflict)
        );
        let first = runtime.request_block(id, peer, 0, 0, 2).unwrap();
        assert!(!runtime.receive_block(id, &second, b"ta").unwrap());
        assert!(runtime.receive_block(id, &first, b"da").unwrap());
        assert_eq!(
            runtime.piece_status(id, 0).unwrap(),
            Some(PieceStatus::Verified)
        );
        assert_eq!(runtime.service().get(id).unwrap().verified_bytes, 4);
        assert_eq!(
            std::fs::read(
                root.join("downloads")
                    .join(format!("{:032x}", id.0))
                    .join("x")
            )
            .unwrap(),
            b"data"
        );
        drop(runtime);

        let reopened = TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap();
        assert_eq!(
            reopened.piece_status(id, 0).unwrap(),
            Some(PieceStatus::Verified)
        );
        assert_eq!(reopened.service().get(id).unwrap().verified_bytes, 4);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn metadata_promotion_preserves_peer_handshake_sessions() {
        let root = root();
        let runtime = TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap();
        let bytes = metainfo();
        let hash = metainfo::parse(&bytes, Default::default())
            .unwrap()
            .info_hash
            .0;
        let hash_hex = hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let magnet = format!("magnet:?xt=urn:btih:{hash_hex}");
        let id = runtime.add_magnet(&magnet).unwrap();
        let peer = [8; 32];
        let handshake = Handshake {
            reserved: [0; 8],
            info_hash: hash,
            peer_id: [9; 20],
        };
        runtime
            .accept_peer_handshake(id, peer, &i2pr_tc_core::wire::encode_handshake(&handshake))
            .unwrap();
        assert_eq!(runtime.service().add_metainfo(&bytes).unwrap(), id);
        runtime.refresh_metainfo(id).unwrap();
        assert_eq!(
            runtime
                .process_peer_message(id, peer, Message::Bitfield(vec![0x80]))
                .unwrap()
                .event,
            PeerEvent::Bitfield(vec![true])
        );
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_requires_requested_bounded_nonoverlapping_blocks() {
        let root = root();
        let runtime = TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap();
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime
            .service()
            .command(TorrentCommand::Start(id))
            .unwrap();
        let peer = [9; 32];
        runtime.set_peer_availability(id, peer, &[true]).unwrap();
        let request = runtime.request_block(id, peer, 0, 0, 2).unwrap();
        assert_eq!(
            runtime.receive_block(id, &request, b"d"),
            Err(ServiceError::InvalidInput)
        );
        assert_eq!(runtime.receive_block(id, &request, b"da"), Ok(false));
        assert_eq!(
            runtime.receive_block(id, &request, b"da"),
            Err(ServiceError::Conflict)
        );
        assert_eq!(
            runtime.piece_status(id, 0).unwrap(),
            Some(PieceStatus::Missing)
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stop_invalidates_outstanding_block_ownership() {
        let root = root();
        let runtime = TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap();
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let peer = [10; 32];
        runtime.set_peer_availability(id, peer, &[true]).unwrap();
        let request = runtime.request_block(id, peer, 0, 0, 4).unwrap();
        runtime.command(TorrentCommand::Stop(id)).unwrap();
        assert_eq!(
            runtime.receive_block(id, &request, b"data"),
            Err(ServiceError::Conflict)
        );
        assert_eq!(
            runtime.piece_status(id, 0).unwrap(),
            Some(PieceStatus::Missing)
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn choke_cancels_peer_owned_blocks_and_allows_peer_retry() {
        let root = root();
        let runtime = TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap();
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let meta = runtime.service().get_metainfo(id).unwrap().unwrap();
        let peers = [[31; 32], [32; 32]];
        for (n, peer) in peers.iter().enumerate() {
            let handshake = Handshake {
                reserved: [0; 8],
                info_hash: meta.info_hash.0,
                peer_id: [n as u8 + 1; 20],
            };
            runtime
                .accept_peer_handshake(id, *peer, &i2pr_tc_core::wire::encode_handshake(&handshake))
                .unwrap();
            runtime
                .process_peer_message(id, *peer, Message::Unchoke)
                .unwrap();
            runtime
                .process_peer_message(id, *peer, Message::Bitfield(vec![0x80]))
                .unwrap();
        }
        let request = runtime.request_block(id, peers[0], 0, 0, 4).unwrap();
        let choked = runtime
            .process_peer_message(id, peers[0], Message::Choke)
            .unwrap();
        assert_eq!(
            choked.cancellations,
            vec![Message::Cancel {
                index: 0,
                begin: 0,
                length: 4,
            }]
        );
        assert_eq!(
            runtime.receive_block(id, &request, b"data"),
            Err(ServiceError::Conflict)
        );
        let retry = runtime.request_block(id, peers[1], 0, 0, 4).unwrap();
        assert_eq!(retry.peer, peers[1]);
        assert!(runtime.receive_block(id, &retry, b"data").unwrap());
        assert_eq!(runtime.service().get(id).unwrap().verified_bytes, 4);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn disconnect_discards_partial_peer_blocks_before_retry() {
        let root = root();
        let runtime = TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap();
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let first_peer = [33; 32];
        let first = runtime.request_block(id, first_peer, 0, 0, 2).unwrap();
        assert!(!runtime.receive_block(id, &first, b"da").unwrap());
        runtime.disconnect_peer(id, first_peer).unwrap();
        let retry = runtime.request_block(id, [34; 32], 0, 0, 4).unwrap();
        assert!(runtime.receive_block(id, &retry, b"data").unwrap());
        assert_eq!(runtime.service().get(id).unwrap().verified_bytes, 4);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn hash_failure_releases_the_piece_for_a_valid_redownload() {
        let root = root();
        let runtime = TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap();
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let peer = [11; 32];
        runtime.set_peer_availability(id, peer, &[true]).unwrap();
        let invalid = runtime.request_block(id, peer, 0, 0, 4).unwrap();
        assert_eq!(
            runtime.receive_block(id, &invalid, b"evil"),
            Err(ServiceError::InvalidInput)
        );
        assert_eq!(
            runtime.piece_status(id, 0).unwrap(),
            Some(PieceStatus::Missing)
        );
        let valid = runtime.request_block(id, peer, 0, 0, 4).unwrap();
        assert!(runtime.receive_block(id, &valid, b"data").unwrap());
        assert_eq!(
            runtime.piece_status(id, 0).unwrap(),
            Some(PieceStatus::Verified)
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn cancelled_piece_acceptance_releases_runtime_request_without_writing() {
        let root = root();
        let runtime = TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap();
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let peer = [14; 32];
        let meta = runtime.service().get_metainfo(id).unwrap().unwrap();
        let handshake = Handshake {
            reserved: [0; 8],
            info_hash: meta.info_hash.0,
            peer_id: [15; 20],
        };
        runtime
            .accept_peer_handshake(id, peer, &i2pr_tc_core::wire::encode_handshake(&handshake))
            .unwrap();
        runtime
            .process_peer_message(id, peer, Message::Unchoke)
            .unwrap();
        runtime
            .process_peer_message(id, peer, Message::Bitfield(vec![0x80]))
            .unwrap();
        let request = runtime.request_block(id, peer, 0, 0, 4).unwrap();
        let cancellation = Cancellation::default();
        cancellation.cancel();
        assert_eq!(
            runtime.process_peer_message_cancellable(
                id,
                peer,
                Message::Piece {
                    index: request.piece,
                    begin: request.begin,
                    block: b"data".to_vec(),
                },
                &cancellation,
            ),
            Err(ServiceError::Cancelled)
        );
        assert_eq!(
            runtime.piece_status(id, 0).unwrap(),
            Some(PieceStatus::Missing)
        );
        assert_eq!(runtime.service().get(id).unwrap().verified_bytes, 0);
        assert!(!runtime.service().payload_root(id).join("x").exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn peer_wire_events_drive_runtime_availability_and_verified_progress() {
        let root = root();
        let runtime = TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap();
        let bytes = metainfo();
        let id = runtime.add_metainfo(&bytes).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let meta = runtime.service().get_metainfo(id).unwrap().unwrap();
        let peer = [12; 32];
        let handshake = Handshake {
            reserved: [0; 8],
            info_hash: meta.info_hash.0,
            peer_id: [13; 20],
        };
        runtime
            .accept_peer_handshake(id, peer, &i2pr_tc_core::wire::encode_handshake(&handshake))
            .unwrap();
        assert_eq!(
            runtime
                .process_peer_message(id, peer, Message::Unchoke)
                .unwrap()
                .event,
            PeerEvent::Unchoked
        );
        runtime
            .process_peer_message(id, peer, Message::Bitfield(vec![0x80]))
            .unwrap();
        assert_eq!(runtime.choose_piece(id, peer).unwrap(), Some(0));
        let request = runtime.request_block(id, peer, 0, 0, 4).unwrap();
        assert_eq!(request.length, 4);
        let outcome = runtime
            .process_peer_message(
                id,
                peer,
                Message::Piece {
                    index: 0,
                    begin: 0,
                    block: b"data".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(outcome.completed_piece, Some(true));
        assert_eq!(runtime.service().get(id).unwrap().verified_bytes, 4);
        let upload = runtime
            .process_peer_message(
                id,
                peer,
                Message::Request {
                    index: 0,
                    begin: 1,
                    length: 2,
                },
            )
            .unwrap();
        assert_eq!(
            upload.event,
            PeerEvent::UploadRequest {
                index: 0,
                begin: 1,
                length: 2
            }
        );
        assert_eq!(runtime.read_verified_block(id, 0, 1, 2).unwrap(), b"at");
        let _ = std::fs::remove_dir_all(root);
    }

    /// A test executor that runs a job on a controlled thread and lets the test
    /// decide exactly when the blocking storage work starts and finishes.
    ///
    /// This is the deterministic seam for the race matrix: nothing here depends
    /// on sleeps racing each other.
    struct GatedExecutor {
        started: Arc<(Mutex<bool>, std::sync::Condvar)>,
        release: Arc<(Mutex<bool>, std::sync::Condvar)>,
        runs: AtomicU64,
        capacity: usize,
    }

    impl GatedExecutor {
        fn new(capacity: usize) -> Arc<Self> {
            Arc::new(Self {
                started: Arc::new((Mutex::new(false), std::sync::Condvar::new())),
                release: Arc::new((Mutex::new(false), std::sync::Condvar::new())),
                runs: AtomicU64::new(0),
                capacity,
            })
        }

        /// Wait until a submitted job is actually running inside the blocking
        /// storage layer.
        fn await_started(&self) {
            let (lock, condvar) = &*self.started;
            let mut started = lock.lock().unwrap();
            while !*started {
                started = condvar.wait(started).unwrap();
            }
        }

        fn release(&self) {
            let (lock, condvar) = &*self.release;
            let mut released = lock.lock().unwrap();
            *released = true;
            condvar.notify_all();
        }

        fn runs(&self) -> u64 {
            self.runs.load(Ordering::SeqCst)
        }
    }

    impl StorageExecutor for GatedExecutor {
        fn spawn_blocking(
            &self,
            job: crate::executor::BlockingJob,
        ) -> Result<(), crate::StorageError> {
            if self.runs() >= self.capacity as u64 {
                return Err(crate::StorageError::Backpressure);
            }
            self.runs.fetch_add(1, Ordering::SeqCst);
            let started = Arc::clone(&self.started);
            let release = Arc::clone(&self.release);
            std::thread::spawn(move || {
                {
                    let (lock, condvar) = &*started;
                    let mut flag = lock.lock().unwrap();
                    *flag = true;
                    condvar.notify_all();
                }
                let (lock, condvar) = &*release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = condvar.wait(released).unwrap();
                }
                job();
            });
            Ok(())
        }
    }

    /// A ready-to-deliver single piece request for `piece`.
    fn piece_request(
        runtime: &TorrentRuntime,
        id: TorrentId,
        piece: u32,
        peer: [u8; 32],
    ) -> BlockRequest {
        let snapshot = runtime.service().get(id).unwrap();
        let handshake = Handshake {
            reserved: [0; 8],
            info_hash: snapshot.info_hash,
            peer_id: [peer[0]; 20],
        };
        runtime
            .accept_peer_handshake(id, peer, &i2pr_tc_core::wire::encode_handshake(&handshake))
            .unwrap();
        let total = runtime
            .service()
            .get_metainfo(id)
            .unwrap()
            .map_or(1, |meta| meta.piece_hashes.len().max(1));
        runtime
            .set_peer_availability(id, peer, &vec![true; total.max(1)])
            .unwrap();
        runtime
            .process_peer_message(id, peer, Message::Unchoke)
            .unwrap();
        runtime
            .process_peer_message(id, peer, Message::Bitfield(vec![0x80]))
            .unwrap();
        assert_eq!(
            runtime.choose_piece(id, peer).unwrap(),
            Some(piece),
            "the scheduler must pick this peer for the piece under test"
        );
        runtime.request_block(id, peer, piece, 0, 4).unwrap()
    }

    /// Drive one complete piece through the offloaded path and return the
    /// outcome, with the test keeping full control of the write's timing.
    async fn offloaded_piece(
        runtime: &TorrentRuntime,
        executor: &Arc<GatedExecutor>,
        id: TorrentId,
        request: &BlockRequest,
        cancellation: &Cancellation,
    ) -> Result<bool, ServiceError> {
        let gate = Arc::clone(executor);
        let future = runtime.receive_block_offloaded(id, request, b"data", cancellation, &*gate);
        tokio::pin!(future);
        let mut started = false;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            tokio::select! {
                biased;
                result = &mut future => return result,
                _ = tokio::time::sleep(std::time::Duration::from_millis(5)), if !started => {
                    if executor.runs() > 0 {
                        started = true;
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    panic!("offloaded persistence did not finish in time");
                }
            }
        }
    }

    // Multi-threaded: the gate below blocks the test thread on a condvar, so a
    // current-thread runtime could never run the task it is waiting for.
    #[tokio::test(flavor = "multi_thread")]
    async fn p1_stop_during_persistence_rejects_the_stale_completion() {
        let root = root();
        let runtime =
            Arc::new(TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap());
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let request = piece_request(&runtime, id, 0, [7; 32]);
        let executor = GatedExecutor::new(4);
        let cancellation = Cancellation::default();
        let runner = tokio::spawn({
            let runtime = Arc::clone(&runtime);
            let runtime = Arc::clone(&runtime);
            let executor = Arc::clone(&executor);
            async move { offloaded_piece(&runtime, &executor, id, &request, &cancellation).await }
        });
        executor.await_started();
        // Re-registering the torrent mints a new generation while the write is
        // still in flight.
        runtime.command(TorrentCommand::Stop(id)).unwrap();
        executor.release();
        let outcome = runner.await.unwrap();
        assert_eq!(
            outcome,
            Ok(false),
            "a stale write must not publish progress"
        );
        assert_eq!(
            runtime.service().get(id).unwrap().verified_bytes,
            0,
            "a stale write must not be counted twice"
        );
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    // Multi-threaded: the gate below blocks the test thread on a condvar, so a
    // current-thread runtime could never run the task it is waiting for.
    #[tokio::test(flavor = "multi_thread")]
    async fn p2_remove_during_persistence_rejects_the_stale_completion() {
        let root = root();
        let runtime =
            Arc::new(TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap());
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let request = piece_request(&runtime, id, 0, [7; 32]);
        let executor = GatedExecutor::new(4);
        let cancellation = Cancellation::default();
        let runner = tokio::spawn({
            let runtime = Arc::clone(&runtime);
            let executor = Arc::clone(&executor);
            async move { offloaded_piece(&runtime, &executor, id, &request, &cancellation).await }
        });
        executor.await_started();
        runtime
            .command(TorrentCommand::Remove {
                id,
                delete_data: false,
            })
            .unwrap();
        executor.release();
        let outcome = runner.await.unwrap();
        assert_eq!(outcome, Ok(false));
        assert_eq!(
            runtime.service().get(id).unwrap_err(),
            ServiceError::NotFound
        );
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    // Multi-threaded: the gate below blocks the test thread on a condvar, so a
    // current-thread runtime could never run the task it is waiting for.
    #[tokio::test(flavor = "multi_thread")]
    async fn p3_cancellation_before_storage_starts_releases_the_request() {
        let root = root();
        let runtime =
            Arc::new(TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap());
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let request = piece_request(&runtime, id, 0, [7; 32]);
        let executor = GatedExecutor::new(4);
        let cancellation = Cancellation::default();
        cancellation.cancel();
        let outcome = runtime
            .receive_block_offloaded(id, &request, b"data", &cancellation, &*executor)
            .await;
        assert!(matches!(outcome, Err(ServiceError::Cancelled)));
        assert_eq!(
            executor.runs(),
            0,
            "no storage work may start after cancellation"
        );
        assert_eq!(
            runtime.piece_status(id, 0).unwrap(),
            Some(PieceStatus::Missing)
        );
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    // Multi-threaded: the gate below blocks the test thread on a condvar, so a
    // current-thread runtime could never run the task it is waiting for.
    #[tokio::test(flavor = "multi_thread")]
    async fn p4_cancellation_during_storage_releases_the_piece_for_retry() {
        let root = root();
        let runtime =
            Arc::new(TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap());
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let request = piece_request(&runtime, id, 0, [7; 32]);
        let executor = GatedExecutor::new(4);
        let cancellation = Cancellation::default();
        let runner = tokio::spawn({
            let runtime = Arc::clone(&runtime);
            let executor = Arc::clone(&executor);
            let token = cancellation.clone();
            let block = request.clone();
            async move {
                runtime
                    .receive_block_offloaded(id, &block, b"data", &token, &*executor)
                    .await
            }
        });
        executor.await_started();
        cancellation.cancel();
        executor.release();
        let outcome = runner.await.unwrap();
        assert!(
            matches!(outcome, Err(ServiceError::Cancelled)),
            "{outcome:?}"
        );
        assert_eq!(
            runtime.piece_status(id, 0).unwrap(),
            Some(PieceStatus::Missing),
            "a cancelled write must leave the piece retryable"
        );
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    // Multi-threaded: the gate below blocks the test thread on a condvar, so a
    // current-thread runtime could never run the task it is waiting for.
    #[tokio::test(flavor = "multi_thread")]
    async fn p5_storage_hash_failure_releases_the_piece_for_retry() {
        let root = root();
        let runtime =
            Arc::new(TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap());
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let request = piece_request(&runtime, id, 0, [7; 32]);
        let pool = BlockingStoragePool::new(2, 4).unwrap();
        let cancellation = Cancellation::default();
        // Bytes that do not match the torrent's piece hash.
        let outcome = runtime
            .receive_block_offloaded(id, &request, b"nope", &cancellation, &pool)
            .await;
        assert!(outcome.is_err(), "corrupt bytes must not be accepted");
        assert_eq!(
            runtime.piece_status(id, 0).unwrap(),
            Some(PieceStatus::Missing)
        );
        assert_eq!(runtime.service().get(id).unwrap().verified_bytes, 0);
        // The reservation was released, so a correct retry still succeeds.
        let retry = piece_request(&runtime, id, 0, [9; 32]);
        assert_eq!(
            runtime
                .receive_block_offloaded(id, &retry, b"data", &cancellation, &pool)
                .await,
            Ok(true)
        );
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    // Multi-threaded: the gate below blocks the test thread on a condvar, so a
    // current-thread runtime could never run the task it is waiting for.
    #[tokio::test(flavor = "multi_thread")]
    async fn p6_duplicate_block_during_persistence_is_rejected() {
        let root = root();
        let runtime =
            Arc::new(TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap());
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let request = piece_request(&runtime, id, 0, [7; 32]);
        let executor = GatedExecutor::new(4);
        let cancellation = Cancellation::default();
        let runner = tokio::spawn({
            let runtime = Arc::clone(&runtime);
            let executor = Arc::clone(&executor);
            let token = cancellation.clone();
            let block = request.clone();
            async move {
                runtime
                    .receive_block_offloaded(id, &block, b"data", &token, &*executor)
                    .await
            }
        });
        executor.await_started();
        let duplicate = runtime
            .receive_block_offloaded(id, &request, b"data", &cancellation, &*executor)
            .await;
        assert!(
            matches!(duplicate, Err(ServiceError::Conflict)),
            "a duplicate block must be refused: {duplicate:?}"
        );
        executor.release();
        assert_eq!(runner.await.unwrap(), Ok(true));
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    // Multi-threaded: the gate below blocks the test thread on a condvar, so a
    // current-thread runtime could never run the task it is waiting for.
    #[tokio::test(flavor = "multi_thread")]
    async fn p7_one_torrents_disk_write_does_not_block_another_torrents_state() {
        let root = root();
        let runtime =
            Arc::new(TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap());
        let first = runtime.add_metainfo(&metainfo()).unwrap();
        let second = runtime.add_metainfo(&metainfo_named("y")).unwrap();
        runtime.command(TorrentCommand::Start(first)).unwrap();
        runtime.command(TorrentCommand::Start(second)).unwrap();
        let request = piece_request(&runtime, first, 0, [7; 32]);
        let executor = GatedExecutor::new(4);
        let cancellation = Cancellation::default();
        let blocked = tokio::spawn({
            let runtime = Arc::clone(&runtime);
            let executor = Arc::clone(&executor);
            async move {
                runtime
                    .receive_block_offloaded(first, &request, b"data", &cancellation, &*executor)
                    .await
            }
        });
        executor.await_started();

        // While the first torrent is blocked inside its blocking write, an
        // unrelated torrent must still make complete state progress.
        let other = piece_request(&runtime, second, 0, [8; 32]);
        // The synchronous entry point is enough: if any global state lock were
        // still held across the first torrent's write, this call could not
        // return while the gated job is parked inside the blocking layer.
        assert_eq!(runtime.receive_block(second, &other, b"data"), Ok(true));
        assert_eq!(
            runtime.piece_status(second, 0).unwrap(),
            Some(PieceStatus::Verified)
        );

        executor.release();
        assert_eq!(blocked.await.unwrap(), Ok(true));
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    // Multi-threaded: the gate below blocks the test thread on a condvar, so a
    // current-thread runtime could never run the task it is waiting for.
    #[tokio::test(flavor = "multi_thread")]
    async fn p8_offloaded_persistence_runs_off_the_calling_task() {
        let root = root();
        let runtime =
            Arc::new(TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap());
        let id = runtime.add_metainfo(&metainfo()).unwrap();
        runtime.command(TorrentCommand::Start(id)).unwrap();
        let request = piece_request(&runtime, id, 0, [7; 32]);
        let pool = BlockingStoragePool::new(2, 4).unwrap();
        let cancellation = Cancellation::default();
        assert_eq!(
            runtime
                .receive_block_offloaded(id, &request, b"data", &cancellation, &pool)
                .await,
            Ok(true)
        );
        assert_eq!(
            runtime.service().get(id).unwrap().status,
            TorrentStatus::Completed
        );
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    // Multi-threaded: the gate below blocks the test thread on a condvar, so a
    // current-thread runtime could never run the task it is waiting for.
    #[tokio::test(flavor = "multi_thread")]
    async fn p9_bounded_executor_applies_backpressure_instead_of_queueing() {
        let root = root();
        let runtime =
            Arc::new(TorrentRuntime::open(&root, &Cancellation::default(), 4, 8, 4).unwrap());
        let first = runtime.add_metainfo(&metainfo()).unwrap();
        let second = runtime.add_metainfo(&metainfo_named("y")).unwrap();
        runtime.command(TorrentCommand::Start(first)).unwrap();
        runtime.command(TorrentCommand::Start(second)).unwrap();
        // Capacity of one: while the first job is parked inside the executor, the
        // second submission must be refused outright rather than queued.
        let executor = GatedExecutor::new(1);
        let cancellation = Cancellation::default();
        let first_request = piece_request(&runtime, first, 0, [7; 32]);
        let second_request = piece_request(&runtime, second, 0, [8; 32]);
        let runner = tokio::spawn({
            let runtime = Arc::clone(&runtime);
            let executor = Arc::clone(&executor);
            let token = cancellation.clone();
            let block = first_request.clone();
            async move {
                runtime
                    .receive_block_offloaded(first, &block, b"data", &token, &*executor)
                    .await
            }
        });
        executor.await_started();
        let refused = runtime
            .receive_block_offloaded(second, &second_request, b"data", &cancellation, &*executor)
            .await;
        assert!(
            refused.is_err(),
            "a full bounded queue must refuse work, not queue it: {refused:?}"
        );
        // The refused torrent kept its piece, so the work is retryable.
        assert_eq!(
            runtime.piece_status(second, 0).unwrap(),
            Some(PieceStatus::Missing)
        );
        executor.release();
        assert_eq!(runner.await.unwrap(), Ok(true));
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }
}
