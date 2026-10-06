//! Durable torrent catalog. It owns intent and metadata records, while piece
//! payload files remain separately rooted by `Storage`.
use crate::{Cancellation, ResumeState, Storage, StorageError};
use i2pr_tc_core::{
    magnet,
    metainfo::{self, InfoHashV1},
    service::{
        EventBatch, FilePriority, ServiceError, ServiceEvent, ServiceEventKind, TorrentCommand,
        TorrentId, TorrentService, TorrentSnapshot, TorrentStatus, MAX_EVENT_BATCH,
        MAX_SERVICE_EVENTS,
    },
};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

const MAX_CATALOG_RECORDS: usize = 100_000;
const MAX_MAGNET_BYTES: usize = 4096;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum Source {
    Metainfo,
    Magnet(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    snapshot: TorrentSnapshot,
    source: Source,
}

#[derive(Default)]
struct State {
    records: BTreeMap<TorrentId, Record>,
    events: VecDeque<ServiceEvent>,
    sequence: u64,
}

/// A filesystem-backed implementation of the native service contract.
/// `root` must be the authorized private application data directory.
pub struct PersistentTorrentService {
    root: PathBuf,
    directory: PathBuf,
    state: Mutex<State>,
}

impl PersistentTorrentService {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StorageError> {
        fs::create_dir_all(root.as_ref())?;
        let root = fs::canonicalize(root)?;
        let directory = root.join("torrents");
        if directory.exists() && fs::symlink_metadata(&directory)?.file_type().is_symlink() {
            return Err(StorageError::Path);
        }
        fs::create_dir_all(&directory)?;
        let mut records = BTreeMap::new();
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|x| x.to_str()) != Some("json") {
                continue;
            }
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(StorageError::Path);
            }
            if metadata.len() > 16 * 1024 {
                return Err(StorageError::Resume);
            }
            if records.len() >= MAX_CATALOG_RECORDS {
                return Err(StorageError::Resume);
            }
            let mut bytes = Vec::with_capacity(metadata.len() as usize);
            File::open(&path)?
                .take(16 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() > 16 * 1024 {
                return Err(StorageError::Resume);
            }
            let mut record: Record =
                serde_json::from_slice(&bytes).map_err(|_| StorageError::Resume)?;
            let id = record.snapshot.id;
            if id != torrent_id(record.snapshot.info_hash) || records.contains_key(&id) {
                return Err(StorageError::Resume);
            }
            let expected_name = record_path(&directory, id);
            if path != expected_name {
                return Err(StorageError::Resume);
            }
            match &record.source {
                Source::Metainfo => {
                    let meta_path = metainfo_path(&directory, id);
                    let metadata = fs::symlink_metadata(&meta_path)?;
                    if metadata.file_type().is_symlink()
                        || !metadata.is_file()
                        || metadata.len() > metainfo::MetaLimits::default().encoded as u64
                    {
                        return Err(StorageError::Resume);
                    }
                    let meta = fs::read(&meta_path)?;
                    let parsed = metainfo::parse(&meta, Default::default())
                        .map_err(|_| StorageError::Resume)?;
                    if parsed.info_hash.0 != record.snapshot.info_hash
                        || parsed.total_length != record.snapshot.total_bytes
                    {
                        return Err(StorageError::Resume);
                    }
                }
                Source::Magnet(uri) => {
                    if uri.len() > MAX_MAGNET_BYTES
                        || magnet::parse(uri, Default::default())
                            .map_err(|_| StorageError::Resume)?
                            .info_hash
                            .0
                            != record.snapshot.info_hash
                    {
                        return Err(StorageError::Resume);
                    }
                }
            }
            match &record.source {
                Source::Metainfo => {
                    let meta_path = metainfo_path(&directory, id);
                    let parsed = metainfo::parse(&fs::read(meta_path)?, Default::default())
                        .map_err(|_| StorageError::Resume)?;
                    if (record.snapshot.verified_pieces.len() != parsed.piece_hashes.len()
                        && !record.snapshot.verified_pieces.is_empty())
                        || record.snapshot.file_priorities.len() != parsed.files.len()
                    {
                        return Err(StorageError::Resume);
                    }
                    if record.snapshot.verified_pieces.is_empty() {
                        record.snapshot.verified_pieces = vec![false; parsed.piece_hashes.len()];
                    }
                    record.snapshot.verified_pieces.fill(false);
                }
                Source::Magnet(_) => {
                    if !record.snapshot.verified_pieces.is_empty()
                        || !record.snapshot.file_priorities.is_empty()
                    {
                        return Err(StorageError::Resume);
                    }
                }
            }
            record.snapshot.verified_bytes = 0;
            record.snapshot.status = match (record.snapshot.status, record.snapshot.desired_running)
            {
                (TorrentStatus::Checking | TorrentStatus::Completed, _) => TorrentStatus::Checking,
                (_, true) => TorrentStatus::Starting,
                _ => TorrentStatus::Stopped,
            };
            records.insert(id, record);
        }
        Ok(Self {
            root,
            directory,
            state: Mutex::new(State {
                records,
                ..State::default()
            }),
        })
    }

    fn insert(
        &self,
        mut record: Record,
        metainfo: Option<&[u8]>,
    ) -> Result<TorrentId, ServiceError> {
        let id = record.snapshot.id;
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        if let Some(existing) = state.records.get(&id) {
            if existing.snapshot.info_hash != record.snapshot.info_hash {
                return Err(ServiceError::Conflict);
            }
            if metainfo.is_none() || matches!(&existing.source, Source::Metainfo) {
                return Ok(id);
            }
            record.snapshot.status = existing.snapshot.status;
            record.snapshot.desired_running = existing.snapshot.desired_running;
            record.snapshot.downloaded_bytes = existing.snapshot.downloaded_bytes;
            record.snapshot.uploaded_bytes = existing.snapshot.uploaded_bytes;
            record.snapshot.download_limit = existing.snapshot.download_limit;
            record.snapshot.upload_limit = existing.snapshot.upload_limit;
            for (new, old) in record
                .snapshot
                .file_priorities
                .iter_mut()
                .zip(&existing.snapshot.file_priorities)
            {
                *new = *old;
            }
            if let Some(bytes) = metainfo {
                atomic_write(&metainfo_path(&self.directory, id), bytes)
                    .map_err(|_| ServiceError::Storage)?;
            }
            if atomic_json(&record_path(&self.directory, id), &record).is_err() {
                return Err(ServiceError::Storage);
            }
            state.records.insert(id, record);
            emit(&mut state, id, ServiceEventKind::MetadataAvailable)?;
            return Ok(id);
        }
        if state.records.len() >= MAX_CATALOG_RECORDS {
            return Err(ServiceError::InvalidInput);
        }
        if let Some(bytes) = metainfo {
            atomic_write(&metainfo_path(&self.directory, id), bytes)
                .map_err(|_| ServiceError::Storage)?;
        }
        let path = record_path(&self.directory, id);
        atomic_json(&path, &record).map_err(|_| ServiceError::Storage)?;
        state.records.insert(id, record);
        emit(&mut state, id, ServiceEventKind::TorrentAdded)?;
        Ok(id)
    }
}

impl TorrentService for PersistentTorrentService {
    fn add_metainfo(&self, bytes: &[u8]) -> Result<TorrentId, ServiceError> {
        if bytes.len() > metainfo::MetaLimits::default().encoded {
            return Err(ServiceError::InvalidInput);
        }
        let meta =
            metainfo::parse(bytes, Default::default()).map_err(|_| ServiceError::InvalidInput)?;
        let record = Record {
            snapshot: TorrentSnapshot {
                id: torrent_id(meta.info_hash.0),
                info_hash: meta.info_hash.0,
                name: meta.name,
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
            source: Source::Metainfo,
        };
        self.insert(record, Some(bytes))
    }

    fn add_magnet(&self, uri: &str) -> Result<TorrentId, ServiceError> {
        if uri.len() > MAX_MAGNET_BYTES {
            return Err(ServiceError::InvalidInput);
        }
        let parsed =
            magnet::parse(uri, Default::default()).map_err(|_| ServiceError::InvalidInput)?;
        let record = Record {
            snapshot: TorrentSnapshot {
                id: torrent_id(parsed.info_hash.0),
                info_hash: parsed.info_hash.0,
                name: parsed
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
            source: Source::Magnet(uri.to_owned()),
        };
        self.insert(record, None)
    }

    fn get_metainfo(&self, id: TorrentId) -> Result<Option<metainfo::TorrentMeta>, ServiceError> {
        let state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        let record = state.records.get(&id).ok_or(ServiceError::NotFound)?;
        if !matches!(&record.source, Source::Metainfo) {
            return Ok(None);
        }
        let path = metainfo_path(&self.directory, id);
        let metadata = fs::symlink_metadata(&path).map_err(|_| ServiceError::Storage)?;
        let max = metainfo::MetaLimits::default().encoded as u64;
        if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > max {
            return Err(ServiceError::Storage);
        }
        let mut bytes = Vec::new();
        File::open(path)
            .map_err(|_| ServiceError::Storage)?
            .take(max + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| ServiceError::Storage)?;
        if bytes.len() as u64 > max {
            return Err(ServiceError::Storage);
        }
        let meta =
            metainfo::parse(&bytes, Default::default()).map_err(|_| ServiceError::Storage)?;
        if meta.info_hash.0 != record.snapshot.info_hash
            || meta.total_length != record.snapshot.total_bytes
        {
            return Err(ServiceError::Storage);
        }
        Ok(Some(meta))
    }

    fn list(&self) -> Result<Vec<TorrentSnapshot>, ServiceError> {
        let state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        Ok(state.records.values().map(|r| r.snapshot.clone()).collect())
    }
    fn get(&self, id: TorrentId) -> Result<TorrentSnapshot, ServiceError> {
        self.state
            .lock()
            .map_err(|_| ServiceError::Storage)?
            .records
            .get(&id)
            .map(|r| r.snapshot.clone())
            .ok_or(ServiceError::NotFound)
    }
    fn command(&self, command: TorrentCommand) -> Result<(), ServiceError> {
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        let target = match &command {
            TorrentCommand::Start(id)
            | TorrentCommand::Stop(id)
            | TorrentCommand::Verify(id)
            | TorrentCommand::Reannounce(id) => *id,
            TorrentCommand::Remove { id, .. } | TorrentCommand::SetLimits { id, .. } => *id,
            TorrentCommand::SetFilePriorities { id, .. } => *id,
        };
        let previous = state.records.get(&target).cloned();
        let (id, kind) = match command {
            TorrentCommand::Start(id) => {
                let record = state.records.get_mut(&id).ok_or(ServiceError::NotFound)?;
                if !matches!(
                    record.snapshot.status,
                    TorrentStatus::Stopped | TorrentStatus::Starting | TorrentStatus::Completed
                ) {
                    return Err(ServiceError::Conflict);
                }
                record.snapshot.status = TorrentStatus::Running;
                record.snapshot.desired_running = true;
                (id, ServiceEventKind::StatusChanged(TorrentStatus::Running))
            }
            TorrentCommand::Stop(id) => {
                let record = state.records.get_mut(&id).ok_or(ServiceError::NotFound)?;
                if !matches!(
                    record.snapshot.status,
                    TorrentStatus::Starting | TorrentStatus::Running | TorrentStatus::Checking
                ) {
                    return Err(ServiceError::Conflict);
                }
                record.snapshot.status = TorrentStatus::Stopped;
                record.snapshot.desired_running = false;
                (id, ServiceEventKind::StatusChanged(TorrentStatus::Stopped))
            }
            TorrentCommand::Verify(id) => {
                let record = state.records.get_mut(&id).ok_or(ServiceError::NotFound)?;
                if record.snapshot.status == TorrentStatus::Checking {
                    return Err(ServiceError::Conflict);
                }
                record.snapshot.status = TorrentStatus::Checking;
                record.snapshot.verified_bytes = 0;
                record.snapshot.verified_pieces.fill(false);
                (id, ServiceEventKind::StatusChanged(TorrentStatus::Checking))
            }
            TorrentCommand::Remove { id, delete_data } => {
                let record = state.records.get(&id).ok_or(ServiceError::NotFound)?;
                if delete_data {
                    if !matches!(&record.source, Source::Metainfo) {
                        return Err(ServiceError::Unsupported);
                    }
                    let meta_path = metainfo_path(&self.directory, id);
                    let metadata =
                        fs::symlink_metadata(&meta_path).map_err(|_| ServiceError::Storage)?;
                    if metadata.file_type().is_symlink()
                        || !metadata.is_file()
                        || metadata.len() > metainfo::MetaLimits::default().encoded as u64
                    {
                        return Err(ServiceError::Storage);
                    }
                    let mut bytes = Vec::new();
                    File::open(meta_path)
                        .map_err(|_| ServiceError::Storage)?
                        .take(metainfo::MetaLimits::default().encoded as u64 + 1)
                        .read_to_end(&mut bytes)
                        .map_err(|_| ServiceError::Storage)?;
                    let parsed = metainfo::parse(&bytes, Default::default())
                        .map_err(|_| ServiceError::Storage)?;
                    if parsed.info_hash.0 != record.snapshot.info_hash {
                        return Err(ServiceError::Storage);
                    }
                    let payload_root = self.payload_root(id);
                    let download_root = self.root.join("downloads");
                    if download_root.exists()
                        && fs::symlink_metadata(&download_root)
                            .map_err(|_| ServiceError::Storage)?
                            .file_type()
                            .is_symlink()
                    {
                        return Err(ServiceError::Storage);
                    }
                    if download_root.exists()
                        && !fs::symlink_metadata(&download_root)
                            .map_err(|_| ServiceError::Storage)?
                            .is_dir()
                    {
                        return Err(ServiceError::Storage);
                    }
                    if payload_root.exists() {
                        let payload_metadata = fs::symlink_metadata(&payload_root)
                            .map_err(|_| ServiceError::Storage)?;
                        if payload_metadata.file_type().is_symlink() || !payload_metadata.is_dir() {
                            return Err(ServiceError::Storage);
                        }
                        let storage = crate::Storage::open(&payload_root)
                            .map_err(|_| ServiceError::Storage)?;
                        storage
                            .remove_data(&parsed.files)
                            .map_err(|_| ServiceError::Storage)?;
                    }
                }
                fs::remove_file(record_path(&self.directory, id))
                    .map_err(|_| ServiceError::Storage)?;
                let meta = metainfo_path(&self.directory, id);
                if meta.exists() {
                    let _ = fs::remove_file(meta);
                }
                state.records.remove(&id);
                (id, ServiceEventKind::TorrentRemoved)
            }
            TorrentCommand::Reannounce(id) => {
                if !state.records.contains_key(&id) {
                    return Err(ServiceError::NotFound);
                }
                (id, ServiceEventKind::ReannounceRequested)
            }
            TorrentCommand::SetLimits {
                id,
                download_bytes_per_second,
                upload_bytes_per_second,
            } => {
                let record = state.records.get_mut(&id).ok_or(ServiceError::NotFound)?;
                record.snapshot.download_limit = download_bytes_per_second;
                record.snapshot.upload_limit = upload_bytes_per_second;
                (id, ServiceEventKind::LimitsChanged)
            }
            TorrentCommand::SetFilePriorities { id, updates } => {
                let record = state.records.get_mut(&id).ok_or(ServiceError::NotFound)?;
                if updates.is_empty() || updates.len() > record.snapshot.file_priorities.len() {
                    return Err(ServiceError::InvalidInput);
                }
                let mut seen = std::collections::BTreeSet::new();
                for (index, _) in &updates {
                    if *index as usize >= record.snapshot.file_priorities.len()
                        || !seen.insert(*index)
                    {
                        return Err(ServiceError::InvalidInput);
                    }
                }
                for (index, priority) in updates {
                    record.snapshot.file_priorities[index as usize] = priority;
                }
                (id, ServiceEventKind::PrioritiesChanged)
            }
        };
        if !matches!(
            kind,
            ServiceEventKind::TorrentRemoved | ServiceEventKind::ReannounceRequested
        ) {
            let record = state.records.get(&id).ok_or(ServiceError::NotFound)?;
            if atomic_json(&record_path(&self.directory, id), record).is_err() {
                if let Some(previous) = previous {
                    state.records.insert(id, previous);
                }
                return Err(ServiceError::Storage);
            }
        }
        emit(&mut state, id, kind)
    }
    fn find_by_hash(&self, hash: InfoHashV1) -> Result<Option<TorrentId>, ServiceError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| ServiceError::Storage)?
            .records
            .values()
            .find(|r| r.snapshot.info_hash == hash.0)
            .map(|r| r.snapshot.id))
    }
    fn store_verified_piece(
        &self,
        id: TorrentId,
        piece: u32,
        bytes: &[u8],
    ) -> Result<(), ServiceError> {
        let meta = self.get_metainfo(id)?.ok_or(ServiceError::Unsupported)?;
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        let previous = state
            .records
            .get(&id)
            .cloned()
            .ok_or(ServiceError::NotFound)?;
        let index = piece as usize;
        let expected = *meta
            .piece_hashes
            .get(index)
            .ok_or(ServiceError::InvalidInput)?;
        let offset = (index as u64)
            .checked_mul(meta.piece_length as u64)
            .ok_or(ServiceError::InvalidInput)?;
        let expected_len = (meta.total_length - offset).min(meta.piece_length as u64) as usize;
        if bytes.len() != expected_len || <[u8; 20]>::from(Sha1::digest(bytes)) != expected {
            return Err(ServiceError::InvalidInput);
        }
        let payload = Storage::open(self.payload_root(id)).map_err(|_| ServiceError::Storage)?;
        payload
            .prepare(&meta.files)
            .and_then(|_| {
                payload.write_verified_piece(&meta.files, meta.piece_length, piece, bytes, expected)
            })
            .map_err(|error| match error {
                StorageError::PieceHash | StorageError::Layout => ServiceError::InvalidInput,
                _ => ServiceError::Storage,
            })?;
        let record = state.records.get_mut(&id).ok_or(ServiceError::NotFound)?;
        if !matches!(
            record.snapshot.status,
            TorrentStatus::Checking | TorrentStatus::Running | TorrentStatus::Starting
        ) {
            return Err(ServiceError::Conflict);
        }
        if record.snapshot.verified_pieces[index] {
            return Ok(());
        }
        record.snapshot.verified_pieces[index] = true;
        record.snapshot.verified_bytes = record
            .snapshot
            .verified_bytes
            .checked_add(expected_len as u64)
            .ok_or(ServiceError::Storage)?;
        record.snapshot.status = if record
            .snapshot
            .verified_pieces
            .iter()
            .all(|verified| *verified)
        {
            TorrentStatus::Completed
        } else {
            record.snapshot.status
        };
        let status = record.snapshot.status;
        if atomic_json(&record_path(&self.directory, id), record).is_err() {
            state.records.insert(id, previous);
            return Err(ServiceError::Storage);
        }
        emit(
            &mut state,
            id,
            if status == TorrentStatus::Completed {
                ServiceEventKind::StatusChanged(status)
            } else {
                ServiceEventKind::ProgressChanged
            },
        )
    }
    fn cancel_verification(&self, id: TorrentId) -> Result<(), ServiceError> {
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        let previous = state
            .records
            .get(&id)
            .cloned()
            .ok_or(ServiceError::NotFound)?;
        let record = state.records.get_mut(&id).ok_or(ServiceError::NotFound)?;
        if record.snapshot.status != TorrentStatus::Checking {
            return Err(ServiceError::Conflict);
        }
        record.snapshot.status = if record.snapshot.desired_running {
            TorrentStatus::Running
        } else {
            TorrentStatus::Stopped
        };
        let status = record.snapshot.status;
        if atomic_json(&record_path(&self.directory, id), record).is_err() {
            state.records.insert(id, previous);
            return Err(ServiceError::Storage);
        }
        emit(&mut state, id, ServiceEventKind::StatusChanged(status))
    }
    fn events_since(&self, after_sequence: u64, limit: usize) -> Result<EventBatch, ServiceError> {
        if limit == 0 || limit > MAX_EVENT_BATCH {
            return Err(ServiceError::InvalidInput);
        }
        let state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        let oldest = state.events.front().map(|e| e.sequence);
        Ok(EventBatch {
            cursor_expired: oldest.is_some_and(|first| after_sequence.saturating_add(1) < first),
            oldest_available: oldest,
            latest_sequence: state.sequence,
            events: state
                .events
                .iter()
                .filter(|e| e.sequence > after_sequence)
                .take(limit)
                .cloned()
                .collect(),
        })
    }
}

fn emit(
    state: &mut State,
    torrent_id: TorrentId,
    kind: ServiceEventKind,
) -> Result<(), ServiceError> {
    state.sequence = state.sequence.checked_add(1).ok_or(ServiceError::Storage)?;
    state.events.push_back(ServiceEvent {
        sequence: state.sequence,
        torrent_id,
        kind,
    });
    while state.events.len() > MAX_SERVICE_EVENTS {
        state.events.pop_front();
    }
    Ok(())
}

fn torrent_id(hash: [u8; 20]) -> TorrentId {
    TorrentId(u128::from_be_bytes(hash[..16].try_into().unwrap()))
}
fn record_path(directory: &Path, id: TorrentId) -> PathBuf {
    directory.join(format!("{:032x}.json", id.0))
}
fn metainfo_path(directory: &Path, id: TorrentId) -> PathBuf {
    directory.join(format!("{:032x}.torrent", id.0))
}

impl PersistentTorrentService {
    fn payload_root(&self, id: TorrentId) -> PathBuf {
        self.root.join("downloads").join(format!("{:032x}", id.0))
    }

    /// Verify every stored piece against metainfo before reporting progress.
    /// Resume bits are written only from this storage-derived result.
    pub fn verify_and_recover(
        &self,
        id: TorrentId,
        cancellation: &Cancellation,
    ) -> Result<Vec<bool>, ServiceError> {
        let meta = self.get_metainfo(id)?.ok_or(ServiceError::Unsupported)?;
        if self.get(id)?.status != TorrentStatus::Checking {
            <Self as TorrentService>::command(self, TorrentCommand::Verify(id))?;
        }
        let payload = Storage::open(self.payload_root(id)).map_err(|_| ServiceError::Storage)?;
        payload
            .prepare(&meta.files)
            .map_err(|_| ServiceError::Storage)?;
        let verified = match payload.recheck_cancellable(
            &meta.files,
            meta.piece_length,
            &meta.piece_hashes,
            cancellation,
        ) {
            Ok(bitmap) => bitmap,
            Err(StorageError::Cancelled) => {
                let _ = <Self as TorrentService>::cancel_verification(self, id);
                return Err(ServiceError::Cancelled);
            }
            Err(_) => {
                let _ = <Self as TorrentService>::cancel_verification(self, id);
                return Err(ServiceError::Storage);
            }
        };
        for (index, good) in verified.iter().enumerate() {
            if *good {
                let offset = (index as u64)
                    .checked_mul(meta.piece_length as u64)
                    .ok_or(ServiceError::Storage)?;
                let length = (meta.total_length - offset).min(meta.piece_length as u64);
                let bytes = payload
                    .read_piece(
                        &meta.files,
                        meta.piece_length,
                        index as u32,
                        length as usize,
                    )
                    .map_err(|_| ServiceError::Storage)?;
                <Self as TorrentService>::store_verified_piece(self, id, index as u32, &bytes)?;
            }
        }
        self.finish_recheck(id)?;
        let snapshot = self.get(id)?;
        let resume = ResumeState {
            version: 1,
            info_hash: meta.info_hash.0,
            storage_schema: 1,
            desired_running: snapshot.desired_running,
            verified: snapshot.verified_pieces.clone(),
            downloaded: snapshot.downloaded_bytes,
            uploaded: snapshot.uploaded_bytes,
        };
        Storage::open(&self.root)
            .and_then(|storage| storage.save_resume(&resume))
            .map_err(|_| {
                let _ = <Self as TorrentService>::cancel_verification(self, id);
                ServiceError::Storage
            })?;
        Ok(verified)
    }

    fn finish_recheck(&self, id: TorrentId) -> Result<(), ServiceError> {
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        let previous = state
            .records
            .get(&id)
            .cloned()
            .ok_or(ServiceError::NotFound)?;
        let record = state.records.get_mut(&id).ok_or(ServiceError::NotFound)?;
        if record.snapshot.status == TorrentStatus::Checking {
            record.snapshot.status = if record.snapshot.verified_pieces.iter().all(|piece| *piece) {
                TorrentStatus::Completed
            } else if record.snapshot.desired_running {
                TorrentStatus::Running
            } else {
                TorrentStatus::Stopped
            };
        }
        let status = record.snapshot.status;
        if atomic_json(&record_path(&self.directory, id), record).is_err() {
            state.records.insert(id, previous);
            return Err(ServiceError::Storage);
        }
        emit(&mut state, id, ServiceEventKind::StatusChanged(status))
    }
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<(), StorageError> {
    let mut bytes = Vec::new();
    serde_json::to_writer(&mut bytes, value).map_err(|_| StorageError::Resume)?;
    if bytes.len() > 16 * 1024 {
        return Err(StorageError::Resume);
    }
    atomic_write(path, &bytes)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let directory = path.parent().ok_or(StorageError::Path)?;
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = directory.join(format!(".i2pr-tc-{}-{sequence}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        if let Ok(dir) = File::open(directory) {
            let _ = dir.sync_all();
        }
        Ok::<(), std::io::Error>(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.map_err(StorageError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha1::{Digest, Sha1};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn root() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "i2pr-tc-service-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn meta() -> Vec<u8> {
        let mut bytes = b"d4:infod6:lengthi1e4:name1:x12:piece lengthi1e6:pieces20:".to_vec();
        bytes.extend(Sha1::digest(b"x"));
        bytes.extend_from_slice(b"ee");
        bytes
    }

    #[test]
    fn catalog_reopens_intent_but_never_trusts_saved_verified_bytes() {
        let root = root();
        let service = PersistentTorrentService::open(&root).unwrap();
        let id = service.add_metainfo(&meta()).unwrap();
        service.command(TorrentCommand::Start(id)).unwrap();
        service
            .command(TorrentCommand::SetLimits {
                id,
                download_bytes_per_second: Some(1234),
                upload_bytes_per_second: Some(567),
            })
            .unwrap();
        service
            .command(TorrentCommand::SetFilePriorities {
                id,
                updates: vec![(0, FilePriority::High)],
            })
            .unwrap();
        drop(service);

        let service = PersistentTorrentService::open(&root).unwrap();
        let snapshot = service.get(id).unwrap();
        assert_eq!(snapshot.status, TorrentStatus::Starting);
        assert!(snapshot.desired_running);
        assert_eq!(snapshot.verified_bytes, 0);
        assert_eq!(snapshot.download_limit, Some(1234));
        assert_eq!(snapshot.upload_limit, Some(567));
        assert_eq!(snapshot.file_priorities, [FilePriority::High]);
        service.command(TorrentCommand::Stop(id)).unwrap();
        drop(service);

        let service = PersistentTorrentService::open(&root).unwrap();
        assert_eq!(service.get(id).unwrap().status, TorrentStatus::Stopped);
        assert!(!service.get(id).unwrap().desired_running);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn remove_without_data_removes_durable_record() {
        let root = root();
        let service = PersistentTorrentService::open(&root).unwrap();
        let id = service
            .add_magnet("magnet:?xt=urn:btih:0000000000000000000000000000000000000000")
            .unwrap();
        service
            .command(TorrentCommand::Remove {
                id,
                delete_data: false,
            })
            .unwrap();
        assert_eq!(service.get(id), Err(ServiceError::NotFound));
        drop(service);
        assert!(PersistentTorrentService::open(&root)
            .unwrap()
            .list()
            .unwrap()
            .is_empty());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn only_hash_verified_piece_progress_is_durable() {
        let root = root();
        let service = PersistentTorrentService::open(&root).unwrap();
        let id = service.add_metainfo(&meta()).unwrap();
        service.command(TorrentCommand::Verify(id)).unwrap();
        assert_eq!(
            service.store_verified_piece(id, 0, b"bad"),
            Err(ServiceError::InvalidInput)
        );
        assert_eq!(service.get(id).unwrap().verified_bytes, 0);
        service.store_verified_piece(id, 0, b"x").unwrap();
        assert_eq!(service.get(id).unwrap().status, TorrentStatus::Completed);
        assert_eq!(fs::read(service.payload_root(id).join("x")).unwrap(), b"x");
        drop(service);
        let service = PersistentTorrentService::open(&root).unwrap();
        assert_eq!(service.get(id).unwrap().status, TorrentStatus::Checking);
        assert_eq!(service.get(id).unwrap().verified_bytes, 0);
        assert_eq!(service.get(id).unwrap().verified_pieces, vec![false]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delete_data_removes_only_the_torrent_payload_before_record_removal() {
        let root = root();
        let service = PersistentTorrentService::open(&root).unwrap();
        let id = service.add_metainfo(&meta()).unwrap();
        let payload_root = service.payload_root(id);
        let storage = crate::Storage::open(&payload_root).unwrap();
        let parsed = metainfo::parse(&meta(), Default::default()).unwrap();
        storage.prepare(&parsed.files).unwrap();
        assert!(payload_root.join("x").exists());

        service
            .command(TorrentCommand::Remove {
                id,
                delete_data: true,
            })
            .unwrap();
        assert!(!payload_root.join("x").exists());
        assert_eq!(service.get(id), Err(ServiceError::NotFound));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolved_magnet_metadata_is_durable_and_keeps_the_id() {
        let root = root();
        let service = PersistentTorrentService::open(&root).unwrap();
        let bytes = meta();
        let parsed = metainfo::parse(&bytes, Default::default()).unwrap();
        let hash = parsed
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
        drop(service);

        let service = PersistentTorrentService::open(&root).unwrap();
        assert_eq!(
            service.get_metainfo(id).unwrap().unwrap().info_hash,
            parsed.info_hash
        );
        assert_eq!(
            service.get(id).unwrap().file_priorities,
            [FilePriority::Normal]
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn recovery_rechecks_pieces_before_restoring_verified_progress() {
        let root = root();
        let service = PersistentTorrentService::open(&root).unwrap();
        let bytes = meta();
        let parsed = metainfo::parse(&bytes, Default::default()).unwrap();
        let id = service.add_metainfo(&bytes).unwrap();
        let payload_root = service.payload_root(id);
        let payload = crate::Storage::open(&payload_root).unwrap();
        payload.prepare(&parsed.files).unwrap();
        payload
            .write_verified_piece(
                &parsed.files,
                parsed.piece_length,
                0,
                b"x",
                parsed.piece_hashes[0],
            )
            .unwrap();
        assert_eq!(
            service
                .verify_and_recover(id, &Cancellation::default())
                .unwrap(),
            vec![true]
        );
        assert_eq!(service.get(id).unwrap().verified_bytes, 1);
        drop(service);

        fs::write(payload_root.join("x"), b"y").unwrap();
        let service = PersistentTorrentService::open(&root).unwrap();
        assert_eq!(service.get(id).unwrap().status, TorrentStatus::Checking);
        let cancelled = Cancellation::default();
        cancelled.cancel();
        assert_eq!(
            service.verify_and_recover(id, &cancelled),
            Err(ServiceError::Cancelled)
        );
        assert_eq!(service.get(id).unwrap().status, TorrentStatus::Stopped);
        assert_eq!(
            service
                .verify_and_recover(id, &Cancellation::default())
                .unwrap(),
            vec![false]
        );
        assert_eq!(service.get(id).unwrap().verified_bytes, 0);
        let resume = crate::Storage::open(&root)
            .unwrap()
            .load_resume(parsed.info_hash, 1, 10, 4096)
            .unwrap();
        assert_eq!(resume.verified, vec![false]);
        drop(payload);
        let _ = fs::remove_dir_all(root);
    }
}
