//! Durable torrent catalog. It owns intent and metadata records, while piece
//! payload files remain separately rooted by `Storage`.
use crate::{Cancellation, ResumeState, Storage, StorageError};
use i2pr_tc_core::{
    magnet,
    metainfo::{self, InfoHashV1},
    service::{
        EventBatch, FilePriority, MAX_EVENT_BATCH, MAX_SERVICE_EVENTS, ServiceError, ServiceEvent,
        ServiceEventKind, TorrentCommand, TorrentId, TorrentService, TorrentSnapshot,
        TorrentStatus,
    },
};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

const MAX_CATALOG_RECORDS: usize = 100_000;
const MAX_MAGNET_BYTES: usize = 4096;
const MAX_CATALOG_RECORD_BYTES: usize = 2 * 1024 * 1024;
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
    #[serde(default)]
    magnet_trackers: Vec<String>,
}

#[derive(Default)]
struct State {
    records: BTreeMap<TorrentId, Record>,
    events: VecDeque<ServiceEvent>,
    sequence: u64,
    /// Monotonic counter bumped on every in-memory record mutation.
    revision: u64,
    /// Latest revision produced for each record, used to decide whether a
    /// finished durable write is still the authority for that record.
    revisions: BTreeMap<TorrentId, u64>,
}

impl State {
    fn touch(&mut self, id: TorrentId) -> u64 {
        self.revision = self.revision.saturating_add(1);
        self.revisions.insert(id, self.revision);
        self.revision
    }
    fn revision_of(&self, id: TorrentId) -> u64 {
        self.revisions.get(&id).copied().unwrap_or_default()
    }
}

/// One in-memory record mutation awaiting its durable write.
#[derive(Clone)]
struct CatalogWrite {
    id: TorrentId,
    previous: Option<Record>,
    revision: u64,
}

/// A filesystem-backed implementation of the native service contract.
/// `root` must be the authorized private application data directory.
pub struct PersistentTorrentService {
    root: PathBuf,
    directory: PathBuf,
    state: Mutex<State>,
    /// Canonical-root-keyed handles so every caller over one payload root shares
    /// a single [`Storage::prepare`]/write/read serialization lock.
    storages: Mutex<BTreeMap<PathBuf, Arc<Storage>>>,
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
            if metadata.len() > MAX_CATALOG_RECORD_BYTES as u64 {
                return Err(StorageError::Resume);
            }
            if records.len() >= MAX_CATALOG_RECORDS {
                return Err(StorageError::Resume);
            }
            let mut bytes = Vec::with_capacity(metadata.len() as usize);
            File::open(&path)?
                .take(MAX_CATALOG_RECORD_BYTES as u64 + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() > MAX_CATALOG_RECORD_BYTES {
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
                    let meta =
                        read_bounded_file(&meta_path, metainfo::MetaLimits::default().encoded)?;
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
                    let meta =
                        read_bounded_file(&meta_path, metainfo::MetaLimits::default().encoded)?;
                    let parsed = metainfo::parse(&meta, Default::default())
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
            storages: Mutex::new(BTreeMap::new()),
        })
    }

    /// Return the shared [`Storage`] handle for one payload or catalog root.
    ///
    /// Handles are cached per canonical root so two callers over the same
    /// payload exclude each other through one `disk` lock instead of creating
    /// independent locks. The cache lock is never held across filesystem work.
    fn storage_for(&self, root: &Path) -> Result<Arc<Storage>, StorageError> {
        if let Some(storage) = self
            .storages
            .lock()
            .map_err(|_| StorageError::Concurrency)?
            .get(root)
            .cloned()
        {
            return Ok(storage);
        }
        let storage = Arc::new(Storage::open(root)?);
        let key = storage.root().to_path_buf();
        Ok(self
            .storages
            .lock()
            .map_err(|_| StorageError::Concurrency)?
            .entry(key)
            .or_insert(storage)
            .clone())
    }

    pub(crate) fn payload_storage(&self, id: TorrentId) -> Result<Arc<Storage>, StorageError> {
        self.storage_for(&self.payload_root(id))
    }

    /// Persist one record snapshot outside the catalog lock.
    ///
    /// The snapshot is re-read from memory so a concurrent mutation is never
    /// lost, and a failed write rolls the in-memory record back only while it is
    /// still the exact revision this writer produced. A record removed while the
    /// write was in flight leaves no orphan file behind.
    fn commit_catalog_write(&self, write: &CatalogWrite) -> Result<(), ServiceError> {
        let CatalogWrite {
            id,
            previous,
            revision,
        } = write.clone();
        let snapshot = {
            let state = self.state.lock().map_err(|_| ServiceError::Storage)?;
            state
                .records
                .get(&id)
                .ok_or(ServiceError::NotFound)?
                .clone()
        };
        if atomic_json(&record_path(&self.directory, id), &snapshot).is_err() {
            let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
            if state.revision_of(id) == revision {
                match previous {
                    Some(previous) => {
                        state.records.insert(id, previous);
                        state.touch(id);
                    }
                    None => {
                        state.records.remove(&id);
                        state.revisions.remove(&id);
                    }
                }
            }
            return Err(ServiceError::Storage);
        }
        // Converge on the newest in-memory record: while a concurrent mutation
        // has advanced this record, rewrite its snapshot so the last rename to
        // land always carries the newest state.
        let mut revision = revision;
        loop {
            let next = {
                let state = self.state.lock().map_err(|_| ServiceError::Storage)?;
                match state.records.get(&id) {
                    None => {
                        // Removed while this write was in flight: drop the file
                        // this writer produced so a removed torrent is not
                        // resurrected on the next open.
                        let _ = fs::remove_file(record_path(&self.directory, id));
                        return Err(ServiceError::NotFound);
                    }
                    Some(_) if state.revision_of(id) == revision => return Ok(()),
                    Some(record) => (state.revision_of(id), record.clone()),
                }
            };
            atomic_json(&record_path(&self.directory, id), &next.1)
                .map_err(|_| ServiceError::Storage)?;
            revision = next.0;
        }
    }

    fn insert(
        &self,
        mut record: Record,
        metainfo: Option<&[u8]>,
    ) -> Result<TorrentId, ServiceError> {
        let id = record.snapshot.id;
        // Phase one: decide the durable snapshot under the catalog lock only.
        let promoted = {
            let state = self.state.lock().map_err(|_| ServiceError::Storage)?;
            if !state.records.contains_key(&id) {
                if state.records.len() >= MAX_CATALOG_RECORDS {
                    return Err(ServiceError::InvalidInput);
                }
                drop(state);
                return self.publish_record(id, record, metainfo, false);
            }
            let existing = state.records.get(&id).ok_or(ServiceError::NotFound)?;
            if existing.snapshot.info_hash != record.snapshot.info_hash {
                return Err(ServiceError::Conflict);
            }
            if metainfo.is_none() || matches!(&existing.source, Source::Metainfo) {
                return Ok(id);
            }
            record.magnet_trackers = if existing.magnet_trackers.is_empty() {
                match &existing.source {
                    Source::Magnet(uri) => {
                        magnet::parse(uri, Default::default())
                            .map_err(|_| ServiceError::Storage)?
                            .trackers
                    }
                    Source::Metainfo => Vec::new(),
                }
            } else {
                existing.magnet_trackers.clone()
            };
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
            true
        };
        self.publish_record(id, record, metainfo, promoted)
    }

    /// Write a prepared record snapshot to disk and publish it in memory.
    ///
    /// No catalog lock is held while the record and metainfo files are written.
    fn publish_record(
        &self,
        id: TorrentId,
        record: Record,
        metainfo: Option<&[u8]>,
        promoted: bool,
    ) -> Result<TorrentId, ServiceError> {
        if let Some(bytes) = metainfo {
            atomic_write(&metainfo_path(&self.directory, id), bytes)
                .map_err(|_| ServiceError::Storage)?;
        }
        atomic_json(&record_path(&self.directory, id), &record)
            .map_err(|_| ServiceError::Storage)?;
        let write = {
            let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
            if promoted && !state.records.contains_key(&id) {
                // Removed while this write was in flight; keep the durable files
                // and the catalog consistent with each other.
                drop(state);
                let _ = fs::remove_file(record_path(&self.directory, id));
                if metainfo.is_some() {
                    let _ = fs::remove_file(metainfo_path(&self.directory, id));
                }
                return Err(ServiceError::NotFound);
            }
            let revision = state.touch(id);
            state.records.insert(id, record);
            CatalogWrite {
                id,
                previous: None,
                revision,
            }
        };
        self.commit_catalog_write(&write)?;
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        emit(
            &mut state,
            id,
            if promoted {
                ServiceEventKind::MetadataAvailable
            } else {
                ServiceEventKind::TorrentAdded
            },
        )?;
        Ok(id)
    }

    /// Remove one torrent's durable records and in-memory catalog entry.
    ///
    /// Every payload and catalog filesystem operation runs with no catalog lock
    /// held; the in-memory entry is dropped last so a failed removal leaves the
    /// catalog exactly as it was.
    fn remove_torrent(&self, id: TorrentId, delete_data: bool) -> Result<(), ServiceError> {
        let info_hash = {
            let state = self.state.lock().map_err(|_| ServiceError::Storage)?;
            state
                .records
                .get(&id)
                .ok_or(ServiceError::NotFound)?
                .snapshot
                .info_hash
        };
        let payload_root = self.payload_root(id);
        if delete_data {
            let bytes = read_metainfo_file(&metainfo_path(&self.directory, id))?;
            let parsed =
                metainfo::parse(&bytes, Default::default()).map_err(|_| ServiceError::Storage)?;
            if parsed.info_hash.0 != info_hash {
                return Err(ServiceError::Storage);
            }
            let download_root = self.root.join("downloads");
            for candidate in [download_root, payload_root.clone()] {
                if !candidate.exists() {
                    continue;
                }
                let metadata =
                    fs::symlink_metadata(&candidate).map_err(|_| ServiceError::Storage)?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(ServiceError::Storage);
                }
            }
            if payload_root.exists() {
                // The shared payload handle keeps this removal serialized with
                // in-flight writes for the same payload root.
                self.payload_storage(id)
                    .map_err(|_| ServiceError::Storage)?
                    .remove_data(&parsed.files)
                    .map_err(|_| ServiceError::Storage)?;
            }
        }
        fs::remove_file(record_path(&self.directory, id)).map_err(|_| ServiceError::Storage)?;
        let meta = metainfo_path(&self.directory, id);
        if meta.exists() {
            let _ = fs::remove_file(meta);
        }
        {
            let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
            if state.records.remove(&id).is_none() {
                return Err(ServiceError::NotFound);
            }
            state.touch(id);
        }
        if let Ok(mut storages) = self.storages.lock() {
            storages.remove(&payload_root);
        }
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        emit(&mut state, id, ServiceEventKind::TorrentRemoved)
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
            magnet_trackers: Vec::new(),
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
            magnet_trackers: parsed.trackers,
        };
        self.insert(record, None)
    }

    fn get_metainfo(&self, id: TorrentId) -> Result<Option<metainfo::TorrentMeta>, ServiceError> {
        let (info_hash, total_bytes) = {
            let state = self.state.lock().map_err(|_| ServiceError::Storage)?;
            let record = state.records.get(&id).ok_or(ServiceError::NotFound)?;
            if !matches!(&record.source, Source::Metainfo) {
                return Ok(None);
            }
            (record.snapshot.info_hash, record.snapshot.total_bytes)
        };
        // Metainfo bytes are read and parsed with no catalog lock held.
        let bytes = read_metainfo_file(&metainfo_path(&self.directory, id))?;
        let meta =
            metainfo::parse(&bytes, Default::default()).map_err(|_| ServiceError::Storage)?;
        if meta.info_hash.0 != info_hash || meta.total_length != total_bytes {
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
        let target = match &command {
            TorrentCommand::Start(id)
            | TorrentCommand::Stop(id)
            | TorrentCommand::Verify(id)
            | TorrentCommand::Reannounce(id) => *id,
            TorrentCommand::Remove { id, .. } | TorrentCommand::SetLimits { id, .. } => *id,
            TorrentCommand::SetFilePriorities { id, .. } => *id,
        };
        if let TorrentCommand::Remove { id, delete_data } = command {
            return self.remove_torrent(id, delete_data);
        }
        // Phase one: the in-memory transition, with no filesystem work.
        let (id, kind, write) = {
            let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
            let previous = state.records.get(&target).cloned();
            let (id, kind, persist) = match command {
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
                    (
                        id,
                        ServiceEventKind::StatusChanged(TorrentStatus::Running),
                        true,
                    )
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
                    (
                        id,
                        ServiceEventKind::StatusChanged(TorrentStatus::Stopped),
                        true,
                    )
                }
                TorrentCommand::Verify(id) => {
                    let record = state.records.get_mut(&id).ok_or(ServiceError::NotFound)?;
                    if record.snapshot.status == TorrentStatus::Checking {
                        return Err(ServiceError::Conflict);
                    }
                    record.snapshot.status = TorrentStatus::Checking;
                    record.snapshot.verified_bytes = 0;
                    record.snapshot.verified_pieces.fill(false);
                    (
                        id,
                        ServiceEventKind::StatusChanged(TorrentStatus::Checking),
                        true,
                    )
                }
                TorrentCommand::Reannounce(id) => {
                    if !state.records.contains_key(&id) {
                        return Err(ServiceError::NotFound);
                    }
                    (id, ServiceEventKind::ReannounceRequested, false)
                }
                TorrentCommand::SetLimits {
                    id,
                    download_bytes_per_second,
                    upload_bytes_per_second,
                } => {
                    let record = state.records.get_mut(&id).ok_or(ServiceError::NotFound)?;
                    record.snapshot.download_limit = download_bytes_per_second;
                    record.snapshot.upload_limit = upload_bytes_per_second;
                    (id, ServiceEventKind::LimitsChanged, true)
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
                    (id, ServiceEventKind::PrioritiesChanged, true)
                }
                TorrentCommand::Remove { .. } => unreachable!("remove is routed separately"),
            };
            let write = persist.then(|| CatalogWrite {
                id,
                previous,
                revision: state.touch(id),
            });
            (id, kind, write)
        };
        // Phase two: the durable write, with no catalog lock held.
        if let Some(write) = &write {
            self.commit_catalog_write(write)?;
        }
        // Phase three: the event, emitted only for a persisted transition.
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
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
        self.store_piece_with_cancellation(id, piece, bytes, None)
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
    /// Return metainfo bytes after validating them against the durable catalog
    /// identity. Used by verified metadata exchange/serving.
    pub fn get_metainfo_bytes(&self, id: TorrentId) -> Result<Option<Vec<u8>>, ServiceError> {
        if self.get_metainfo(id)?.is_none() {
            return Ok(None);
        }
        let path = metainfo_path(&self.directory, id);
        let bytes = read_bounded_file(&path, metainfo::MetaLimits::default().encoded)
            .map_err(|_| ServiceError::Storage)?;
        Ok(Some(bytes))
    }

    /// Tracker URLs from metainfo and magnet `tr` parameters, deduplicated
    /// while preserving declaration order.
    pub fn tracker_urls(&self, id: TorrentId) -> Result<Vec<String>, ServiceError> {
        let magnet_trackers = {
            let state = self.state.lock().map_err(|_| ServiceError::Storage)?;
            let record = state.records.get(&id).ok_or(ServiceError::NotFound)?;
            if record.magnet_trackers.is_empty() {
                match &record.source {
                    Source::Magnet(uri) => {
                        magnet::parse(uri, Default::default())
                            .map_err(|_| ServiceError::Storage)?
                            .trackers
                    }
                    Source::Metainfo => Vec::new(),
                }
            } else {
                record.magnet_trackers.clone()
            }
        };
        let mut trackers = self
            .get_metainfo(id)?
            .map(|meta| meta.trackers)
            .unwrap_or_default();
        trackers.extend(magnet_trackers);
        let mut seen = std::collections::BTreeSet::new();
        trackers.retain(|tracker| seen.insert(tracker.clone()));
        Ok(trackers)
    }

    /// Tracker announce tiers from metainfo, or a single tier from magnet `tr`
    /// parameters while metadata is unresolved.
    pub fn tracker_tiers(&self, id: TorrentId) -> Result<Vec<Vec<String>>, ServiceError> {
        let mut tiers = if let Some(bytes) = self.get_metainfo_bytes(id)? {
            metainfo::parse(&bytes, Default::default())
                .map_err(|_| ServiceError::Storage)?
                .tracker_tiers
        } else {
            Vec::new()
        };
        let mut seen: BTreeSet<String> = tiers.iter().flatten().cloned().collect();
        let additional: Vec<_> = self
            .tracker_urls(id)?
            .into_iter()
            .filter(|tracker| seen.insert(tracker.clone()))
            .collect();
        if !additional.is_empty() {
            tiers.push(additional);
        }
        Ok(tiers)
    }

    pub(crate) fn payload_root(&self, id: TorrentId) -> PathBuf {
        self.root.join("downloads").join(format!("{:032x}", id.0))
    }

    pub(crate) fn store_piece_with_cancellation(
        &self,
        id: TorrentId,
        piece: u32,
        bytes: &[u8],
        cancellation: Option<&Cancellation>,
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
        let record = state.records.get(&id).ok_or(ServiceError::NotFound)?;
        if !matches!(
            record.snapshot.status,
            TorrentStatus::Checking | TorrentStatus::Running | TorrentStatus::Starting
        ) {
            return Err(ServiceError::Conflict);
        }
        if record.snapshot.verified_pieces[index] {
            return Ok(());
        }
        if cancellation.is_some_and(Cancellation::is_cancelled) {
            return Err(ServiceError::Cancelled);
        }
        let payload = Storage::open(self.payload_root(id)).map_err(|_| ServiceError::Storage)?;
        payload
            .prepare(&meta.files)
            .map_err(|_| ServiceError::Storage)?;
        let result = if let Some(token) = cancellation {
            payload.write_verified_piece_cancellable(
                &meta.files,
                meta.piece_length,
                piece,
                bytes,
                expected,
                token,
            )
        } else {
            payload.write_verified_piece(&meta.files, meta.piece_length, piece, bytes, expected)
        };
        result.map_err(|error| match error {
            StorageError::Cancelled => ServiceError::Cancelled,
            StorageError::PieceHash | StorageError::Layout => ServiceError::InvalidInput,
            _ => ServiceError::Storage,
        })?;
        if cancellation.is_some_and(Cancellation::is_cancelled) {
            return Err(ServiceError::Cancelled);
        }
        let record = state.records.get_mut(&id).ok_or(ServiceError::NotFound)?;
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
        let payload = match Storage::open(self.payload_root(id)) {
            Ok(payload) => payload,
            Err(_) => {
                let _ = <Self as TorrentService>::cancel_verification(self, id);
                return Err(ServiceError::Storage);
            }
        };
        if payload.prepare(&meta.files).is_err() {
            let _ = <Self as TorrentService>::cancel_verification(self, id);
            return Err(ServiceError::Storage);
        }
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
        let restoration = (|| {
            for (index, good) in verified.iter().enumerate() {
                if cancellation.is_cancelled() {
                    return Err(ServiceError::Cancelled);
                }
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
                    self.store_piece_with_cancellation(
                        id,
                        index as u32,
                        &bytes,
                        Some(cancellation),
                    )?;
                }
            }
            Ok::<(), ServiceError>(())
        })();
        if let Err(error) = restoration {
            let _ = <Self as TorrentService>::cancel_verification(self, id);
            return Err(error);
        }
        if cancellation.is_cancelled() {
            let _ = <Self as TorrentService>::cancel_verification(self, id);
            return Err(ServiceError::Cancelled);
        }
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
        self.finish_recheck(id)?;
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
    if bytes.len() > MAX_CATALOG_RECORD_BYTES {
        return Err(StorageError::Resume);
    }
    atomic_write(path, &bytes)
}

/// Read one torrent's durable metainfo file with the encoded-size bound applied.
///
/// Symlinks, non-files, and oversized records fail closed, and the bound is
/// re-checked after the read so a file grown past the limit during the read is
/// still rejected.
fn read_metainfo_file(path: &Path) -> Result<Vec<u8>, ServiceError> {
    let max = metainfo::MetaLimits::default().encoded;
    let metadata = fs::symlink_metadata(path).map_err(|_| ServiceError::Storage)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > max as u64 {
        return Err(ServiceError::Storage);
    }
    read_bounded_file(path, max).map_err(|_| ServiceError::Storage)
}

fn read_bounded_file(path: &Path, max_bytes: usize) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::with_capacity(max_bytes.min(16 * 1024));
    let limit = u64::try_from(max_bytes)
        .map_err(|_| StorageError::Resume)?
        .checked_add(1)
        .ok_or(StorageError::Resume)?;
    File::open(path)?.take(limit).read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(StorageError::Resume);
    }
    Ok(bytes)
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
        assert!(
            PersistentTorrentService::open(&root)
                .unwrap()
                .list()
                .unwrap()
                .is_empty()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn catalog_rejects_torrent_id_prefix_collisions() {
        let root = root();
        let service = PersistentTorrentService::open(&root).unwrap();
        let id = service
            .add_magnet("magnet:?xt=urn:btih:0000000000000000000000000000000000000000")
            .unwrap();
        let mut snapshot = service.get(id).unwrap();
        snapshot.info_hash = [0x11; 20];
        let collision = Record {
            snapshot,
            source: Source::Magnet(
                "magnet:?xt=urn:btih:1111111111111111111111111111111111111111".into(),
            ),
            magnet_trackers: Vec::new(),
        };
        assert_eq!(service.insert(collision, None), Err(ServiceError::Conflict));
        assert_eq!(service.get(id).unwrap().info_hash, [0; 20]);
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
            .add_magnet(&format!(
                "magnet:?xt=urn:btih:{hash}&dn=pending&tr=http%3A%2F%2Ftracker.i2p%2Fannounce"
            ))
            .unwrap();
        assert!(service.get_metainfo(id).unwrap().is_none());
        assert_eq!(
            service.tracker_urls(id).unwrap(),
            vec!["http://tracker.i2p/announce"]
        );
        assert_eq!(
            service.tracker_tiers(id).unwrap(),
            vec![vec!["http://tracker.i2p/announce".to_owned()]]
        );
        assert_eq!(service.add_metainfo(&bytes).unwrap(), id);
        assert_eq!(service.get(id).unwrap().total_bytes, 1);
        assert_eq!(
            service.tracker_urls(id).unwrap(),
            vec!["http://tracker.i2p/announce"]
        );
        assert_eq!(
            service.tracker_tiers(id).unwrap(),
            vec![vec!["http://tracker.i2p/announce".to_owned()]]
        );
        assert_eq!(service.get_metainfo_bytes(id).unwrap().unwrap(), bytes);
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
        assert_eq!(
            service.tracker_urls(id).unwrap(),
            vec!["http://tracker.i2p/announce"]
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn large_piece_bitmaps_are_rebuilt_instead_of_bloating_catalog_records() {
        let root = root();
        let count = 10_000usize;
        let piece_hashes = vec![0; count * 20];
        let mut bytes = format!(
            "d4:infod6:lengthi{count}e4:name1:x12:piece lengthi1e6:pieces{}:",
            piece_hashes.len()
        )
        .into_bytes();
        bytes.extend_from_slice(&piece_hashes);
        bytes.extend_from_slice(b"ee");
        let service = PersistentTorrentService::open(&root).unwrap();
        let id = service.add_metainfo(&bytes).unwrap();
        assert_eq!(service.get(id).unwrap().verified_pieces.len(), count);
        drop(service);

        let service = PersistentTorrentService::open(&root).unwrap();
        assert_eq!(service.get(id).unwrap().verified_pieces, vec![false; count]);
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
