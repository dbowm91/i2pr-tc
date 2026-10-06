//! Durable torrent catalog. It owns intent and metadata records, while piece
//! payload files remain separately rooted by `Storage`.
use crate::StorageError;
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
            record.snapshot.verified_bytes = 0;
            record.snapshot.status = if record.snapshot.desired_running {
                TorrentStatus::Starting
            } else {
                match record.snapshot.status {
                    TorrentStatus::Completed => TorrentStatus::Checking,
                    _ => TorrentStatus::Stopped,
                }
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

    fn insert(&self, record: Record, metainfo: Option<&[u8]>) -> Result<TorrentId, ServiceError> {
        let id = record.snapshot.id;
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        if let Some(existing) = state.records.get(&id) {
            return if existing.snapshot.info_hash == record.snapshot.info_hash {
                Ok(id)
            } else {
                Err(ServiceError::Conflict)
            };
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
            },
            source: Source::Magnet(uri.to_owned()),
        };
        self.insert(record, None)
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
    fn finish_verification(&self, id: TorrentId, verified_bytes: u64) -> Result<(), ServiceError> {
        let mut state = self.state.lock().map_err(|_| ServiceError::Storage)?;
        let previous = state
            .records
            .get(&id)
            .cloned()
            .ok_or(ServiceError::NotFound)?;
        let record = state.records.get_mut(&id).ok_or(ServiceError::NotFound)?;
        if record.snapshot.status != TorrentStatus::Checking
            || verified_bytes > record.snapshot.total_bytes
        {
            return Err(ServiceError::Conflict);
        }
        record.snapshot.verified_bytes = verified_bytes;
        record.snapshot.status = if verified_bytes == record.snapshot.total_bytes {
            TorrentStatus::Completed
        } else if record.snapshot.desired_running {
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
        bytes.extend([0u8; 20]);
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
    fn verification_completion_is_durable_and_bounded_by_torrent_length() {
        let root = root();
        let service = PersistentTorrentService::open(&root).unwrap();
        let id = service.add_metainfo(&meta()).unwrap();
        service.command(TorrentCommand::Verify(id)).unwrap();
        assert_eq!(
            service.finish_verification(id, 2),
            Err(ServiceError::Conflict)
        );
        service.finish_verification(id, 1).unwrap();
        assert_eq!(service.get(id).unwrap().status, TorrentStatus::Completed);
        drop(service);
        let service = PersistentTorrentService::open(&root).unwrap();
        assert_eq!(service.get(id).unwrap().status, TorrentStatus::Checking);
        assert_eq!(service.get(id).unwrap().verified_bytes, 0);
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
}
