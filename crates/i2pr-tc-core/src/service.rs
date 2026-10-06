//! Native torrent service vocabulary. It deliberately contains no HTTP/RPC types.
use crate::InfoHashV1;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
};

pub const MAX_SERVICE_EVENTS: usize = 1024;
pub const MAX_EVENT_BATCH: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TorrentId(pub u128);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentStatus {
    Stopped,
    Starting,
    Running,
    Checking,
    Error,
    Completed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilePriority {
    Low,
    Normal,
    High,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TorrentSnapshot {
    pub id: TorrentId,
    pub info_hash: [u8; 20],
    pub name: String,
    pub status: TorrentStatus,
    pub desired_running: bool,
    pub total_bytes: u64,
    pub verified_bytes: u64,
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub download_limit: Option<u64>,
    pub upload_limit: Option<u64>,
    pub file_priorities: Vec<FilePriority>,
    #[serde(default)]
    pub verified_pieces: Vec<bool>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceError {
    InvalidInput,
    NotFound,
    Unsupported,
    Conflict,
    Storage,
    Cancelled,
}
#[derive(Clone, Debug)]
pub enum TorrentCommand {
    Start(TorrentId),
    Stop(TorrentId),
    Verify(TorrentId),
    Remove {
        id: TorrentId,
        delete_data: bool,
    },
    Reannounce(TorrentId),
    SetLimits {
        id: TorrentId,
        download_bytes_per_second: Option<u64>,
        upload_bytes_per_second: Option<u64>,
    },
    SetFilePriorities {
        id: TorrentId,
        updates: Vec<(u32, FilePriority)>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceEventKind {
    TorrentAdded,
    StatusChanged(TorrentStatus),
    TorrentRemoved,
    ReannounceRequested,
    LimitsChanged,
    PrioritiesChanged,
    MetadataAvailable,
    ProgressChanged,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceEvent {
    pub sequence: u64,
    pub torrent_id: TorrentId,
    pub kind: ServiceEventKind,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventBatch {
    /// True means `after_sequence` predates the retained ring; refresh via list/get.
    pub cursor_expired: bool,
    pub oldest_available: Option<u64>,
    pub latest_sequence: u64,
    pub events: Vec<ServiceEvent>,
}

pub trait TorrentService: Send + Sync {
    fn add_metainfo(&self, metainfo: &[u8]) -> Result<TorrentId, ServiceError>;
    fn add_magnet(&self, magnet: &str) -> Result<TorrentId, ServiceError>;
    fn get_metainfo(&self, id: TorrentId) -> Result<Option<crate::TorrentMeta>, ServiceError>;
    fn list(&self) -> Result<Vec<TorrentSnapshot>, ServiceError>;
    fn get(&self, id: TorrentId) -> Result<TorrentSnapshot, ServiceError>;
    fn command(&self, command: TorrentCommand) -> Result<(), ServiceError>;
    fn store_verified_piece(
        &self,
        id: TorrentId,
        piece: u32,
        bytes: &[u8],
    ) -> Result<(), ServiceError>;
    fn cancel_verification(&self, id: TorrentId) -> Result<(), ServiceError>;
    fn find_by_hash(&self, hash: InfoHashV1) -> Result<Option<TorrentId>, ServiceError>;
    fn events_since(&self, after_sequence: u64, limit: usize) -> Result<EventBatch, ServiceError>;
}

/// Deterministic in-memory catalog implementing the service contract without network access.
/// Production persistence is provided by the storage-layer service owner.
#[derive(Default)]
pub struct MemoryTorrentService {
    state: Mutex<MemoryState>,
}
#[derive(Default)]
struct MemoryState {
    items: BTreeMap<TorrentId, TorrentSnapshot>,
    metainfo: BTreeMap<TorrentId, crate::TorrentMeta>,
    events: VecDeque<ServiceEvent>,
    latest_sequence: u64,
}

impl MemoryTorrentService {
    fn emit(
        state: &mut MemoryState,
        id: TorrentId,
        kind: ServiceEventKind,
    ) -> Result<(), ServiceError> {
        let sequence = state
            .latest_sequence
            .checked_add(1)
            .ok_or(ServiceError::Storage)?;
        state.latest_sequence = sequence;
        state.events.push_back(ServiceEvent {
            sequence,
            torrent_id: id,
            kind,
        });
        while state.events.len() > MAX_SERVICE_EVENTS {
            state.events.pop_front();
        }
        Ok(())
    }

    fn insert_magnet(&self, magnet: crate::magnet::Magnet) -> Result<TorrentId, ServiceError> {
        let hash = magnet.info_hash;
        let id = TorrentId(u128::from_be_bytes(
            hash.0[..16]
                .try_into()
                .map_err(|_| ServiceError::InvalidInput)?,
        ));
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        if let Some(existing) = state.items.get(&id) {
            if existing.info_hash != hash.0 {
                return Err(ServiceError::Conflict);
            }
            return Ok(id);
        }
        state.items.insert(
            id,
            TorrentSnapshot {
                id,
                info_hash: hash.0,
                name: magnet
                    .display_name
                    .unwrap_or_else(|| "(metadata pending)".into()),
                status: TorrentStatus::Stopped,
                desired_running: false,
                total_bytes: 0,
                verified_bytes: 0,
                downloaded_bytes: 0,
                uploaded_bytes: 0,
                download_limit: None,
                upload_limit: None,
                file_priorities: Vec::new(),
                verified_pieces: Vec::new(),
            },
        );
        Self::emit(&mut state, id, ServiceEventKind::TorrentAdded)?;
        Ok(id)
    }

    fn insert_metainfo(&self, meta: crate::TorrentMeta) -> Result<TorrentId, ServiceError> {
        let id = TorrentId(u128::from_be_bytes(
            meta.info_hash.0[..16]
                .try_into()
                .map_err(|_| ServiceError::InvalidInput)?,
        ));
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        if let Some(existing) = state.items.get(&id) {
            if existing.info_hash != meta.info_hash.0 {
                return Err(ServiceError::Conflict);
            }
            if state.metainfo.contains_key(&id) {
                return Ok(id);
            }
            let snapshot = state.items.get_mut(&id).ok_or(ServiceError::NotFound)?;
            snapshot.name = meta.name.clone();
            snapshot.total_bytes = meta.total_length;
            snapshot.file_priorities = vec![FilePriority::Normal; meta.files.len()];
            snapshot.verified_pieces = vec![false; meta.piece_hashes.len()];
            state.metainfo.insert(id, meta);
            Self::emit(&mut state, id, ServiceEventKind::MetadataAvailable)?;
            return Ok(id);
        }
        state.items.insert(
            id,
            TorrentSnapshot {
                id,
                info_hash: meta.info_hash.0,
                name: meta.name.clone(),
                status: TorrentStatus::Stopped,
                desired_running: false,
                total_bytes: meta.total_length,
                verified_bytes: 0,
                downloaded_bytes: 0,
                uploaded_bytes: 0,
                download_limit: None,
                upload_limit: None,
                file_priorities: vec![FilePriority::Normal; meta.files.len()],
                verified_pieces: vec![false; meta.piece_hashes.len()],
            },
        );
        state.metainfo.insert(id, meta);
        Self::emit(&mut state, id, ServiceEventKind::TorrentAdded)?;
        Ok(id)
    }
}

impl TorrentService for MemoryTorrentService {
    fn add_metainfo(&self, bytes: &[u8]) -> Result<TorrentId, ServiceError> {
        let m = crate::metainfo::parse(bytes, Default::default())
            .map_err(|_| ServiceError::InvalidInput)?;
        self.insert_metainfo(m)
    }
    fn add_magnet(&self, uri: &str) -> Result<TorrentId, ServiceError> {
        let m = crate::magnet::parse(uri, Default::default())
            .map_err(|_| ServiceError::InvalidInput)?;
        self.insert_magnet(m)
    }
    fn get_metainfo(&self, id: TorrentId) -> Result<Option<crate::TorrentMeta>, ServiceError> {
        let state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        if !state.items.contains_key(&id) {
            return Err(ServiceError::NotFound);
        }
        Ok(state.metainfo.get(&id).cloned())
    }
    fn list(&self) -> Result<Vec<TorrentSnapshot>, ServiceError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| ServiceError::Storage)?
            .items
            .values()
            .cloned()
            .collect())
    }
    fn get(&self, id: TorrentId) -> Result<TorrentSnapshot, ServiceError> {
        self.state
            .lock()
            .map_err(|_| ServiceError::Storage)?
            .items
            .get(&id)
            .cloned()
            .ok_or(ServiceError::NotFound)
    }
    fn command(&self, command: TorrentCommand) -> Result<(), ServiceError> {
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        let (id, event) = match command {
            TorrentCommand::Start(id) => {
                let torrent = state.items.get_mut(&id).ok_or(ServiceError::NotFound)?;
                if !matches!(
                    torrent.status,
                    TorrentStatus::Stopped | TorrentStatus::Completed
                ) {
                    return Err(ServiceError::Conflict);
                }
                torrent.status = TorrentStatus::Running;
                torrent.desired_running = true;
                (id, ServiceEventKind::StatusChanged(TorrentStatus::Running))
            }
            TorrentCommand::Stop(id) => {
                let torrent = state.items.get_mut(&id).ok_or(ServiceError::NotFound)?;
                if !matches!(
                    torrent.status,
                    TorrentStatus::Starting | TorrentStatus::Running | TorrentStatus::Checking
                ) {
                    return Err(ServiceError::Conflict);
                }
                torrent.status = TorrentStatus::Stopped;
                torrent.desired_running = false;
                (id, ServiceEventKind::StatusChanged(TorrentStatus::Stopped))
            }
            TorrentCommand::Verify(id) => {
                let torrent = state.items.get_mut(&id).ok_or(ServiceError::NotFound)?;
                if torrent.status == TorrentStatus::Checking {
                    return Err(ServiceError::Conflict);
                }
                torrent.status = TorrentStatus::Checking;
                torrent.verified_bytes = 0;
                torrent.verified_pieces.fill(false);
                (id, ServiceEventKind::StatusChanged(TorrentStatus::Checking))
            }
            TorrentCommand::Remove { id, delete_data } => {
                if delete_data {
                    return Err(ServiceError::Unsupported);
                }
                state.items.remove(&id).ok_or(ServiceError::NotFound)?;
                (id, ServiceEventKind::TorrentRemoved)
            }
            TorrentCommand::Reannounce(id) => {
                if !state.items.contains_key(&id) {
                    return Err(ServiceError::NotFound);
                }
                (id, ServiceEventKind::ReannounceRequested)
            }
            TorrentCommand::SetLimits {
                id,
                download_bytes_per_second,
                upload_bytes_per_second,
            } => {
                let torrent = state.items.get_mut(&id).ok_or(ServiceError::NotFound)?;
                torrent.download_limit = download_bytes_per_second;
                torrent.upload_limit = upload_bytes_per_second;
                (id, ServiceEventKind::LimitsChanged)
            }
            TorrentCommand::SetFilePriorities { id, updates } => {
                let torrent = state.items.get_mut(&id).ok_or(ServiceError::NotFound)?;
                if updates.is_empty() || updates.len() > torrent.file_priorities.len() {
                    return Err(ServiceError::InvalidInput);
                }
                let mut seen = std::collections::BTreeSet::new();
                for (index, _) in &updates {
                    if *index as usize >= torrent.file_priorities.len() || !seen.insert(*index) {
                        return Err(ServiceError::InvalidInput);
                    }
                }
                for (index, priority) in updates {
                    torrent.file_priorities[index as usize] = priority;
                }
                (id, ServiceEventKind::PrioritiesChanged)
            }
        };
        Self::emit(&mut state, id, event)
    }
    fn find_by_hash(&self, hash: InfoHashV1) -> Result<Option<TorrentId>, ServiceError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| ServiceError::Storage)?
            .items
            .values()
            .find(|s| s.info_hash == hash.0)
            .map(|s| s.id))
    }
    fn store_verified_piece(
        &self,
        id: TorrentId,
        piece: u32,
        bytes: &[u8],
    ) -> Result<(), ServiceError> {
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        let meta = state.metainfo.get(&id).ok_or(ServiceError::Unsupported)?;
        let index = piece as usize;
        let Some(expected) = meta.piece_hashes.get(index) else {
            return Err(ServiceError::InvalidInput);
        };
        let offset = (index as u64)
            .checked_mul(meta.piece_length as u64)
            .ok_or(ServiceError::InvalidInput)?;
        let expected_len = (meta.total_length - offset).min(meta.piece_length as u64) as usize;
        if bytes.len() != expected_len || <[u8; 20]>::from(Sha1::digest(bytes)) != *expected {
            return Err(ServiceError::InvalidInput);
        }
        let torrent = state.items.get_mut(&id).ok_or(ServiceError::NotFound)?;
        if !matches!(
            torrent.status,
            TorrentStatus::Running | TorrentStatus::Checking | TorrentStatus::Starting
        ) {
            return Err(ServiceError::Conflict);
        }
        if torrent.verified_pieces[index] {
            return Ok(());
        }
        torrent.verified_pieces[index] = true;
        torrent.verified_bytes = torrent
            .verified_bytes
            .checked_add(expected_len as u64)
            .ok_or(ServiceError::Storage)?;
        let completed = torrent.verified_pieces.iter().all(|verified| *verified);
        let event = if completed {
            torrent.status = TorrentStatus::Completed;
            ServiceEventKind::StatusChanged(TorrentStatus::Completed)
        } else {
            ServiceEventKind::ProgressChanged
        };
        Self::emit(&mut state, id, event)
    }
    fn cancel_verification(&self, id: TorrentId) -> Result<(), ServiceError> {
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        let torrent = state.items.get_mut(&id).ok_or(ServiceError::NotFound)?;
        if torrent.status != TorrentStatus::Checking {
            return Err(ServiceError::Conflict);
        }
        torrent.status = if torrent.desired_running {
            TorrentStatus::Running
        } else {
            TorrentStatus::Stopped
        };
        let status = torrent.status;
        Self::emit(&mut state, id, ServiceEventKind::StatusChanged(status))
    }
    fn events_since(&self, after_sequence: u64, limit: usize) -> Result<EventBatch, ServiceError> {
        if limit == 0 || limit > MAX_EVENT_BATCH {
            return Err(ServiceError::InvalidInput);
        }
        let state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        let oldest = state.events.front().map(|event| event.sequence);
        Ok(EventBatch {
            cursor_expired: oldest.is_some_and(|first| after_sequence.saturating_add(1) < first),
            oldest_available: oldest,
            latest_sequence: state.latest_sequence,
            events: state
                .events
                .iter()
                .filter(|event| event.sequence > after_sequence)
                .take(limit)
                .cloned()
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service_with_one_torrent() -> (MemoryTorrentService, TorrentId) {
        let service = MemoryTorrentService::default();
        let id = service
            .add_magnet("magnet:?xt=urn:btih:0000000000000000000000000000000000000000&dn=sample")
            .unwrap();
        (service, id)
    }

    #[test]
    fn commands_update_snapshots_and_publish_ordered_events() {
        let (service, id) = service_with_one_torrent();
        assert_eq!(service.get(id).unwrap().status, TorrentStatus::Stopped);
        assert_eq!(service.command(TorrentCommand::Start(id)), Ok(()));
        assert_eq!(
            service.command(TorrentCommand::Start(id)),
            Err(ServiceError::Conflict)
        );
        service
            .command(TorrentCommand::SetLimits {
                id,
                download_bytes_per_second: Some(12),
                upload_bytes_per_second: None,
            })
            .unwrap();
        let snapshot = service.get(id).unwrap();
        assert_eq!(snapshot.status, TorrentStatus::Running);
        assert_eq!(snapshot.download_limit, Some(12));
        let events = service.events_since(0, 16).unwrap();
        assert!(!events.cursor_expired);
        assert_eq!(events.latest_sequence, 3);
        assert_eq!(events.events[0].kind, ServiceEventKind::TorrentAdded);
        assert_eq!(
            events.events[1].kind,
            ServiceEventKind::StatusChanged(TorrentStatus::Running)
        );
        assert_eq!(events.events[2].kind, ServiceEventKind::LimitsChanged);
    }

    #[test]
    fn unsupported_delete_and_invalid_event_bounds_are_truthful() {
        let (service, id) = service_with_one_torrent();
        assert_eq!(
            service.command(TorrentCommand::Remove {
                id,
                delete_data: true
            }),
            Err(ServiceError::Unsupported)
        );
        assert!(service.get(id).is_ok());
        assert_eq!(service.events_since(0, 0), Err(ServiceError::InvalidInput));
        assert_eq!(
            service.events_since(0, MAX_EVENT_BATCH + 1),
            Err(ServiceError::InvalidInput)
        );
    }

    #[test]
    fn verified_piece_updates_progress_only_after_hash_check() {
        let service = MemoryTorrentService::default();
        let mut bytes = b"d4:infod6:lengthi1e4:name1:x12:piece lengthi1e6:pieces20:".to_vec();
        bytes.extend(Sha1::digest(b"x"));
        bytes.extend_from_slice(b"ee");
        let id = service.add_metainfo(&bytes).unwrap();
        service.command(TorrentCommand::Verify(id)).unwrap();
        assert_eq!(
            service.store_verified_piece(id, 0, b"wrong"),
            Err(ServiceError::InvalidInput)
        );
        assert_eq!(service.get(id).unwrap().verified_bytes, 0);
        service.store_verified_piece(id, 0, b"x").unwrap();
        assert_eq!(service.get(id).unwrap().verified_bytes, 1);
        assert_eq!(service.get(id).unwrap().status, TorrentStatus::Completed);
    }

    #[test]
    fn file_priorities_are_bounded_and_native() {
        let service = MemoryTorrentService::default();
        let mut bytes = b"d4:infod6:lengthi1e4:name1:x12:piece lengthi1e6:pieces20:".to_vec();
        bytes.extend([0; 20]);
        bytes.extend_from_slice(b"ee");
        let id = service.add_metainfo(&bytes).unwrap();
        service
            .command(TorrentCommand::SetFilePriorities {
                id,
                updates: vec![(0, FilePriority::High)],
            })
            .unwrap();
        assert_eq!(
            service.get(id).unwrap().file_priorities,
            [FilePriority::High]
        );
        assert_eq!(
            service.command(TorrentCommand::SetFilePriorities {
                id,
                updates: vec![(0, FilePriority::Low), (0, FilePriority::High)],
            }),
            Err(ServiceError::InvalidInput)
        );
        assert_eq!(
            service.get(id).unwrap().file_priorities,
            [FilePriority::High]
        );
    }

    #[test]
    fn metadata_promotes_an_existing_magnet_without_changing_its_id() {
        let service = MemoryTorrentService::default();
        let mut bytes = b"d4:infod6:lengthi1e4:name1:x12:piece lengthi1e6:pieces20:".to_vec();
        bytes.extend([0; 20]);
        bytes.extend_from_slice(b"ee");
        let meta = crate::metainfo::parse(&bytes, Default::default()).unwrap();
        let hash = meta
            .info_hash
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let id = service
            .add_magnet(&format!("magnet:?xt=urn:btih:{hash}&dn=pending"))
            .unwrap();
        assert!(service.get_metainfo(id).unwrap().is_none());
        assert_eq!(service.add_metainfo(&bytes).unwrap(), id);
        assert_eq!(service.get(id).unwrap().total_bytes, 1);
        assert_eq!(
            service.get_metainfo(id).unwrap().unwrap().info_hash,
            meta.info_hash
        );
        let events = service.events_since(0, 8).unwrap();
        assert_eq!(events.events[1].kind, ServiceEventKind::MetadataAvailable);
    }

    #[test]
    fn event_ring_reports_expired_cursor() {
        let (service, id) = service_with_one_torrent();
        for _ in 0..MAX_SERVICE_EVENTS {
            service.command(TorrentCommand::Reannounce(id)).unwrap();
        }
        let events = service.events_since(0, 4).unwrap();
        assert!(events.cursor_expired);
        assert_eq!(events.events.len(), 4);
        assert_eq!(events.oldest_available, Some(2));
    }
}
