//! Single-owner composition of peer block scheduling, verified storage, and service progress.
use crate::{Cancellation, PersistentTorrentService, Storage};
use i2pr_tc_core::{
    magnet, metainfo,
    metainfo::InfoHashV1,
    service::{ServiceError, TorrentCommand, TorrentId, TorrentService, TorrentStatus},
    state::{BlockRequest, PieceMap, PieceStatus, ScheduleError},
    wire::{Handshake, Message, PeerEvent, PeerWireSession},
};
use std::{collections::BTreeMap, path::Path, sync::Mutex};

const MAX_RUNTIME_PIECES: usize = 4_000_000;
const MAX_BLOCK_LENGTH: u32 = 16 * 1024;
const MAX_BUFFERED_BLOCK_BYTES: usize = 64 * 1024 * 1024;

struct TorrentRuntimeState {
    piece_length: u32,
    total_length: u64,
    pieces: PieceMap,
    requests: BTreeMap<(u32, u32, [u8; 32]), BlockRequest>,
    blocks: BTreeMap<u32, Vec<(u32, Vec<u8>)>>,
    buffered_bytes: usize,
    peers: BTreeMap<[u8; 32], PeerWireSession>,
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
    service: PersistentTorrentService,
    torrents: Mutex<BTreeMap<TorrentId, TorrentRuntimeState>>,
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
        let mut torrents = BTreeMap::new();
        for snapshot in snapshots {
            let Some(meta) = service.get_metainfo(snapshot.id)? else {
                continue;
            };
            let mut pieces = PieceMap::new(
                meta.piece_hashes.len(),
                MAX_RUNTIME_PIECES,
                max_inflight_per_torrent,
                max_inflight_per_peer,
            )
            .map_err(map_schedule)?;
            if snapshot.verified_pieces.len() != meta.piece_hashes.len() {
                return Err(ServiceError::Storage);
            }
            for (index, verified) in snapshot.verified_pieces.iter().enumerate() {
                if *verified {
                    pieces.mark_verified(index as u32).map_err(map_schedule)?;
                }
            }
            torrents.insert(
                snapshot.id,
                TorrentRuntimeState {
                    piece_length: meta.piece_length,
                    total_length: meta.total_length,
                    pieces,
                    requests: BTreeMap::new(),
                    blocks: BTreeMap::new(),
                    buffered_bytes: 0,
                    peers: BTreeMap::new(),
                },
            );
        }
        Ok(Self {
            service,
            torrents: Mutex::new(torrents),
            max_torrents,
            max_inflight_per_torrent,
            max_inflight_per_peer,
        })
    }

    pub fn service(&self) -> &PersistentTorrentService {
        &self.service
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
        self.service.add_magnet(uri)
    }

    /// Call after metadata for a magnet has been received and promoted in the service.
    pub fn refresh_metainfo(&self, id: TorrentId) -> Result<(), ServiceError> {
        if self.service.get_metainfo(id)?.is_none() {
            return Err(ServiceError::Unsupported);
        }
        self.register(id)
    }

    pub fn set_peer_availability(
        &self,
        id: TorrentId,
        peer: [u8; 32],
        pieces: &[bool],
    ) -> Result<(), ServiceError> {
        let mut torrents = self.torrents.lock().map_err(|_| ServiceError::Storage)?;
        let state = torrents.get_mut(&id).ok_or(ServiceError::NotFound)?;
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
        let meta = self
            .service
            .get_metainfo(id)?
            .ok_or(ServiceError::Unsupported)?;
        let mut session = PeerWireSession::new(
            meta.info_hash.0,
            meta.piece_length,
            meta.total_length,
            MAX_BLOCK_LENGTH,
            self.max_inflight_per_peer,
        )
        .map_err(|_| ServiceError::InvalidInput)?;
        let handshake = session
            .accept_handshake(bytes)
            .map_err(|_| ServiceError::InvalidInput)?;
        let mut torrents = self.torrents.lock().map_err(|_| ServiceError::Storage)?;
        let state = torrents.get_mut(&id).ok_or(ServiceError::NotFound)?;
        if state.peers.contains_key(&peer) {
            return Err(ServiceError::Conflict);
        }
        state
            .pieces
            .set_availability(peer, &vec![false; meta.piece_hashes.len()])
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
        let (event, cancellations, incoming_block) = {
            let mut torrents = self.torrents.lock().map_err(|_| ServiceError::Storage)?;
            let state = torrents.get_mut(&id).ok_or(ServiceError::NotFound)?;
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
            Some(self.receive_block(id, &request, &bytes)?)
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
        let mut torrents = self.torrents.lock().map_err(|_| ServiceError::Storage)?;
        let state = torrents.get_mut(&id).ok_or(ServiceError::NotFound)?;
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
        let torrents = self.torrents.lock().map_err(|_| ServiceError::Storage)?;
        Ok(torrents
            .get(&id)
            .ok_or(ServiceError::NotFound)?
            .pieces
            .choose_piece(peer))
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
        let mut torrents = self.torrents.lock().map_err(|_| ServiceError::Storage)?;
        let state = torrents.get_mut(&id).ok_or(ServiceError::NotFound)?;
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
        if bytes.len() != request.length as usize {
            return Err(ServiceError::InvalidInput);
        }
        let mut torrents = self.torrents.lock().map_err(|_| ServiceError::Storage)?;
        let state = torrents.get_mut(&id).ok_or(ServiceError::NotFound)?;
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
                return Ok(false);
            }
            assembled.extend_from_slice(block);
        }
        if assembled.len() != piece_size {
            return Ok(false);
        }
        let piece = request.piece;
        match self.service.store_verified_piece(id, piece, &assembled) {
            Ok(()) => {
                state.pieces.mark_verified(piece).map_err(map_schedule)?;
                if let Some(blocks) = state.blocks.remove(&piece) {
                    state.buffered_bytes -=
                        blocks.iter().map(|(_, block)| block.len()).sum::<usize>();
                }
                state.requests.retain(|(p, _, _), _| *p != piece);
                Ok(true)
            }
            Err(error) => {
                state.pieces.reset_piece(piece).map_err(map_schedule)?;
                if let Some(blocks) = state.blocks.remove(&piece) {
                    state.buffered_bytes -=
                        blocks.iter().map(|(_, block)| block.len()).sum::<usize>();
                }
                state.requests.retain(|(p, _, _), _| *p != piece);
                Err(error)
            }
        }
    }

    pub fn piece_status(
        &self,
        id: TorrentId,
        piece: u32,
    ) -> Result<Option<PieceStatus>, ServiceError> {
        let torrents = self.torrents.lock().map_err(|_| ServiceError::Storage)?;
        Ok(torrents
            .get(&id)
            .ok_or(ServiceError::NotFound)?
            .pieces
            .status(piece))
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
        let storage =
            Storage::open(self.service.payload_root(id)).map_err(|_| ServiceError::Storage)?;
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
        let meta = self
            .service
            .get_metainfo(id)?
            .ok_or(ServiceError::Unsupported)?;
        let snapshot = self.service.get(id)?;
        let mut pieces = PieceMap::new(
            meta.piece_hashes.len(),
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
        self.torrents
            .lock()
            .map_err(|_| ServiceError::Storage)?
            .insert(
                id,
                TorrentRuntimeState {
                    piece_length: meta.piece_length,
                    total_length: meta.total_length,
                    pieces,
                    requests: BTreeMap::new(),
                    blocks: BTreeMap::new(),
                    buffered_bytes: 0,
                    peers: BTreeMap::new(),
                },
            );
        Ok(())
    }
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
    use i2pr_tc_core::service::TorrentCommand;
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
        let mut bytes = b"d4:infod6:lengthi4e4:name1:x12:piece lengthi4e6:pieces20:".to_vec();
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
}
