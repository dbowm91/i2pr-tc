//! Stable Transmission integer IDs backed by a bounded atomic catalog.
use i2pr_tc_core::service::TorrentId;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use thiserror::Error;

const MAX_ID_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_IDS: usize = 100_000;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IdError {
    #[error("stable RPC ID catalog is invalid or over its bound")]
    Invalid,
    #[error("stable RPC ID catalog I/O failed")]
    Io,
    #[error("stable RPC ID catalog lock is poisoned")]
    Lock,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdRecord {
    version: u8,
    next: i64,
    entries: BTreeMap<String, i64>,
}

pub struct RpcIdStore {
    path: PathBuf,
    state: Mutex<IdRecord>,
}

impl RpcIdStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, IdError> {
        let path = path.as_ref().to_owned();
        let record = if path.exists() {
            let metadata = fs::metadata(&path).map_err(|_| IdError::Io)?;
            if metadata.len() > MAX_ID_FILE_BYTES {
                return Err(IdError::Invalid);
            }
            let bytes = fs::read(&path).map_err(|_| IdError::Io)?;
            serde_json::from_slice::<IdRecord>(&bytes).map_err(|_| IdError::Invalid)?
        } else {
            IdRecord {
                version: 1,
                next: 1,
                entries: BTreeMap::new(),
            }
        };
        validate_record(&record)?;
        Ok(Self {
            path,
            state: Mutex::new(record),
        })
    }

    pub fn id_for(&self, id: TorrentId) -> Result<i64, IdError> {
        let key = format!("{:032x}", id.0);
        let mut state = self.state.lock().map_err(|_| IdError::Lock)?;
        if let Some(rpc_id) = state.entries.get(&key) {
            return Ok(*rpc_id);
        }
        if state.entries.len() >= MAX_IDS || state.next <= 0 || state.next == i64::MAX {
            return Err(IdError::Invalid);
        }
        let rpc_id = state.next;
        let mut updated = state.clone();
        updated.entries.insert(key, rpc_id);
        updated.next = rpc_id + 1;
        persist(&self.path, &updated)?;
        *state = updated;
        Ok(rpc_id)
    }

    pub fn torrent_for(&self, rpc_id: i64) -> Result<Option<TorrentId>, IdError> {
        if rpc_id <= 0 {
            return Ok(None);
        }
        let state = self.state.lock().map_err(|_| IdError::Lock)?;
        state
            .entries
            .iter()
            .find(|(_, value)| **value == rpc_id)
            .map(|(key, _)| parse_torrent_id(key))
            .transpose()
    }
}

fn validate_record(record: &IdRecord) -> Result<(), IdError> {
    if record.version != 1 || record.next <= 0 || record.entries.len() > MAX_IDS {
        return Err(IdError::Invalid);
    }
    let mut values = BTreeSet::new();
    for (key, value) in &record.entries {
        if key.len() != 32
            || !key.bytes().all(|byte| byte.is_ascii_hexdigit())
            || *value <= 0
            || *value >= record.next
            || !values.insert(*value)
        {
            return Err(IdError::Invalid);
        }
    }
    Ok(())
}

fn parse_torrent_id(value: &str) -> Result<TorrentId, IdError> {
    u128::from_str_radix(value, 16)
        .map(TorrentId)
        .map_err(|_| IdError::Invalid)
}

fn persist(path: &Path, record: &IdRecord) -> Result<(), IdError> {
    let bytes = serde_json::to_vec(record).map_err(|_| IdError::Invalid)?;
    if bytes.len() as u64 > MAX_ID_FILE_BYTES {
        return Err(IdError::Invalid);
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|_| IdError::Io)?;
    }
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = path.with_extension(format!("rpc-ids-{}-{sequence}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .map_err(|_| IdError::Io)?;
    let result = (|| {
        file.write_all(&bytes).map_err(|_| IdError::Io)?;
        file.sync_all().map_err(|_| IdError::Io)?;
        fs::rename(&temp, path).map_err(|_| IdError::Io)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn path() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "i2pr-tc-rpc-ids-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn ids_survive_reopen_and_are_never_reused() {
        let root = path();
        let file = root.join("ids.json");
        let first = TorrentId(0x1234);
        let second = TorrentId(0x5678);
        let store = RpcIdStore::open(&file).unwrap();
        let first_id = store.id_for(first).unwrap();
        let second_id = store.id_for(second).unwrap();
        assert_ne!(first_id, second_id);
        drop(store);
        let store = RpcIdStore::open(&file).unwrap();
        assert_eq!(store.id_for(first).unwrap(), first_id);
        assert_eq!(store.torrent_for(second_id).unwrap(), Some(second));
        drop(store);
        let _ = fs::remove_dir_all(root);
    }
}
