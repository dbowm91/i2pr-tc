//! Native torrent service vocabulary. It deliberately contains no HTTP/RPC types.
use crate::InfoHashV1;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Mutex};

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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TorrentSnapshot {
    pub id: TorrentId,
    pub info_hash: [u8; 20],
    pub name: String,
    pub status: TorrentStatus,
    pub total_bytes: u64,
    pub verified_bytes: u64,
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceError {
    InvalidInput,
    NotFound,
    Unsupported,
    Conflict,
    Storage,
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
}
pub trait TorrentService: Send + Sync {
    fn add_metainfo(&self, metainfo: &[u8]) -> Result<TorrentId, ServiceError>;
    fn add_magnet(&self, magnet: &str) -> Result<TorrentId, ServiceError>;
    fn list(&self) -> Result<Vec<TorrentSnapshot>, ServiceError>;
    fn get(&self, id: TorrentId) -> Result<TorrentSnapshot, ServiceError>;
    fn command(&self, command: TorrentCommand) -> Result<(), ServiceError>;
    fn find_by_hash(&self, hash: InfoHashV1) -> Result<Option<TorrentId>, ServiceError>;
}

/// Deterministic catalog implementing the native API without network access.
/// It provides the frozen contract for storage and transport adapters.
#[derive(Default)]
pub struct MemoryTorrentService {
    items: Mutex<BTreeMap<TorrentId, TorrentSnapshot>>,
}
impl MemoryTorrentService {
    fn insert(
        &self,
        hash: InfoHashV1,
        name: String,
        total: u64,
    ) -> Result<TorrentId, ServiceError> {
        let id = TorrentId(u128::from_be_bytes(
            hash.0[..16]
                .try_into()
                .map_err(|_| ServiceError::InvalidInput)?,
        ));
        let mut items = self.items.lock().map_err(|_| ServiceError::Storage)?;
        if let Some(existing) = items.get(&id) {
            if existing.info_hash != hash.0 {
                return Err(ServiceError::Conflict);
            }
            return Ok(id);
        }
        items.insert(
            id,
            TorrentSnapshot {
                id,
                info_hash: hash.0,
                name,
                status: TorrentStatus::Stopped,
                total_bytes: total,
                verified_bytes: 0,
                downloaded_bytes: 0,
                uploaded_bytes: 0,
            },
        );
        Ok(id)
    }
}
impl TorrentService for MemoryTorrentService {
    fn add_metainfo(&self, bytes: &[u8]) -> Result<TorrentId, ServiceError> {
        let m = crate::metainfo::parse(bytes, Default::default())
            .map_err(|_| ServiceError::InvalidInput)?;
        self.insert(m.info_hash, m.name, m.total_length)
    }
    fn add_magnet(&self, uri: &str) -> Result<TorrentId, ServiceError> {
        let m = crate::magnet::parse(uri, Default::default())
            .map_err(|_| ServiceError::InvalidInput)?;
        self.insert(
            m.info_hash,
            m.display_name
                .unwrap_or_else(|| "(metadata pending)".into()),
            0,
        )
    }
    fn list(&self) -> Result<Vec<TorrentSnapshot>, ServiceError> {
        Ok(self
            .items
            .lock()
            .map_err(|_| ServiceError::Storage)?
            .values()
            .cloned()
            .collect())
    }
    fn get(&self, id: TorrentId) -> Result<TorrentSnapshot, ServiceError> {
        self.items
            .lock()
            .map_err(|_| ServiceError::Storage)?
            .get(&id)
            .cloned()
            .ok_or(ServiceError::NotFound)
    }
    fn command(&self, command: TorrentCommand) -> Result<(), ServiceError> {
        let mut items = self.items.lock().map_err(|_| ServiceError::Storage)?;
        match command {
            TorrentCommand::Start(id) => {
                items.get_mut(&id).ok_or(ServiceError::NotFound)?.status = TorrentStatus::Running
            }
            TorrentCommand::Stop(id) => {
                items.get_mut(&id).ok_or(ServiceError::NotFound)?.status = TorrentStatus::Stopped
            }
            TorrentCommand::Verify(id) => {
                items.get_mut(&id).ok_or(ServiceError::NotFound)?.status = TorrentStatus::Checking
            }
            TorrentCommand::Remove { id, .. } => {
                items.remove(&id).ok_or(ServiceError::NotFound)?;
            }
            TorrentCommand::Reannounce(_) | TorrentCommand::SetLimits { .. } => {
                return Err(ServiceError::Unsupported)
            }
        }
        Ok(())
    }
    fn find_by_hash(&self, hash: InfoHashV1) -> Result<Option<TorrentId>, ServiceError> {
        Ok(self
            .items
            .lock()
            .map_err(|_| ServiceError::Storage)?
            .values()
            .find(|s| s.info_hash == hash.0)
            .map(|s| s.id))
    }
}
