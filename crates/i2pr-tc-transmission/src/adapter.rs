//! Transmission RPC methods mapped onto the native `TorrentRuntime` contract.
use crate::{
    ids::{IdError, RpcIdStore},
    wire::{self, RpcRequest, WireMode},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use fs2::available_space;
use i2pr_tc_core::service::{
    FilePriority, ServiceError, TorrentCommand, TorrentService, TorrentSnapshot, TorrentStatus,
};
use i2pr_tc_core::{magnet, metainfo};
use i2pr_tc_storage::{Cancellation, TorrentRuntime};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use thiserror::Error;

pub const RPC_VERSION_SEMVER: &str = "6.0.0";
pub const ADAPTER_VERSION: &str = env!("CARGO_PKG_VERSION");
const MAX_TORRENTS_PER_REQUEST: usize = 4096;
const MAX_FIELDS: usize = 64;
const MAX_VERIFY_JOBS: usize = 16;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RpcFault {
    #[error("method not found")]
    MethodNotFound,
    #[error("invalid parameters")]
    InvalidParams,
    #[error("torrent not found")]
    NotFound,
    #[error("requested feature is unsupported")]
    Unsupported,
    #[error("torrent service rejected the request")]
    Service,
    #[error("persistent RPC ID mapping failed")]
    IdStore,
}

impl From<ServiceError> for RpcFault {
    fn from(error: ServiceError) -> Self {
        match error {
            ServiceError::NotFound => Self::NotFound,
            ServiceError::Unsupported => Self::Unsupported,
            ServiceError::InvalidInput | ServiceError::Conflict | ServiceError::Cancelled => {
                Self::InvalidParams
            }
            ServiceError::Storage => Self::Service,
        }
    }
}

impl From<IdError> for RpcFault {
    fn from(_: IdError) -> Self {
        Self::IdStore
    }
}

pub struct TransmissionAdapter {
    runtime: Arc<TorrentRuntime>,
    ids: Arc<RpcIdStore>,
    data_root: PathBuf,
    mutations: Mutex<()>,
    verify_jobs: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
pub struct RpcResponse {
    pub mode: WireMode,
    pub id: Option<Value>,
    pub result: Result<Value, RpcFault>,
}

pub fn encode_response(response: &RpcResponse) -> Result<Vec<u8>, wire::WireError> {
    let value = match response.mode {
        WireMode::JsonRpc => match &response.result {
            Ok(result) => json!({"jsonrpc": "2.0", "id": response.id, "result": result}),
            Err(error) => json!({
                "jsonrpc": "2.0",
                "id": response.id,
                "error": {"code": fault_code(error), "message": error.to_string()}
            }),
        },
        WireMode::Legacy => {
            let (result, arguments) = match &response.result {
                Ok(value) => ("success", value.clone()),
                Err(error) => ("failure", json!({"result": error.to_string()})),
            };
            json!({"result": result, "arguments": arguments, "tag": response.id})
        }
    };
    let bytes = serde_json::to_vec(&value).map_err(|_| wire::WireError::Invalid)?;
    if bytes.len() > wire::MAX_RESPONSE_BYTES {
        return Err(wire::WireError::Limit);
    }
    Ok(bytes)
}

fn fault_code(error: &RpcFault) -> i32 {
    match error {
        RpcFault::MethodNotFound => -32601,
        RpcFault::InvalidParams => -32602,
        RpcFault::NotFound => -32001,
        RpcFault::Unsupported => -32002,
        RpcFault::Service | RpcFault::IdStore => -32000,
    }
}

impl TransmissionAdapter {
    pub fn new(
        runtime: Arc<TorrentRuntime>,
        id_store: Arc<RpcIdStore>,
        data_root: impl AsRef<Path>,
    ) -> Result<Self, RpcFault> {
        let data_root = std::fs::canonicalize(data_root).map_err(|_| RpcFault::Service)?;
        if !data_root.is_dir() {
            return Err(RpcFault::Service);
        }
        Ok(Self {
            runtime,
            ids: id_store,
            data_root,
            mutations: Mutex::new(()),
            verify_jobs: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn handle(&self, bytes: &[u8]) -> Result<RpcResponse, wire::WireError> {
        let request = wire::parse_request(bytes)?;
        let result = self.dispatch(&request);
        Ok(RpcResponse {
            mode: request.mode,
            id: request.id,
            result,
        })
    }

    fn dispatch(&self, request: &RpcRequest) -> Result<Value, RpcFault> {
        let method = wire::canonical_method(request.mode, &request.method)
            .map_err(|_| RpcFault::MethodNotFound)?;
        let params = canonical_params(&request.params)?;
        let _mutation_guard = if is_mutating(method) {
            Some(self.mutations.lock().map_err(|_| RpcFault::Service)?)
        } else {
            None
        };
        match method {
            "session_get" => self.session_get(request.mode, &params),
            "session_stats" => self.session_stats(request.mode),
            "torrent_get" => self.torrent_get(request.mode, &params),
            "torrent_add" => self.torrent_add(request.mode, &params),
            "torrent_set" => self.torrent_set(&params),
            "torrent_start" | "torrent_start_now" => {
                self.torrent_action(&params, TorrentAction::Start)
            }
            "torrent_stop" => self.torrent_action(&params, TorrentAction::Stop),
            "torrent_verify" => self.torrent_action(&params, TorrentAction::Verify),
            "torrent_reannounce" => self.torrent_action(&params, TorrentAction::Reannounce),
            "torrent_remove" => self.torrent_remove(&params),
            "free_space" => self.free_space(&params),
            _ => Err(RpcFault::MethodNotFound),
        }
    }

    fn session_get(
        &self,
        mode: WireMode,
        params: &BTreeMap<String, Value>,
    ) -> Result<Value, RpcFault> {
        reject_unknown(params, &["fields"])?;
        let all = BTreeMap::from([
            (
                "version",
                Value::String(format!("i2pr-tc/{ADAPTER_VERSION}")),
            ),
            (
                "rpc_version_semver",
                Value::String(RPC_VERSION_SEMVER.into()),
            ),
        ]);
        project_fields(mode, &all, params.get("fields"))
    }

    fn session_stats(&self, mode: WireMode) -> Result<Value, RpcFault> {
        let torrents = self.runtime.service().list().map_err(RpcFault::from)?;
        let active = torrents
            .iter()
            .filter(|item| {
                matches!(
                    item.status,
                    TorrentStatus::Running | TorrentStatus::Starting
                )
            })
            .count();
        let paused = torrents
            .iter()
            .filter(|item| item.status == TorrentStatus::Stopped)
            .count();
        let downloaded = torrents.iter().fold(0u64, |total, item| {
            total.saturating_add(item.downloaded_bytes)
        });
        let uploaded = torrents.iter().fold(0u64, |total, item| {
            total.saturating_add(item.uploaded_bytes)
        });
        let cumulative = json!({"downloaded_bytes": downloaded, "uploaded_bytes": uploaded});
        let all = BTreeMap::from([
            ("torrent_count", json!(torrents.len())),
            ("active_torrent_count", json!(active)),
            ("paused_torrent_count", json!(paused)),
            ("cumulative_stats", cumulative),
        ]);
        Ok(project_keys(mode, &all))
    }

    fn torrent_get(
        &self,
        mode: WireMode,
        params: &BTreeMap<String, Value>,
    ) -> Result<Value, RpcFault> {
        reject_unknown(params, &["ids", "fields"])?;
        let selected = self.select_torrents(params.get("ids"))?;
        let requested = fields(params.get("fields"), mode)?;
        let torrents = selected
            .iter()
            .map(|snapshot| self.project_torrent(snapshot, &requested, mode))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(json!({"torrents": torrents}))
    }

    fn project_torrent(
        &self,
        snapshot: &TorrentSnapshot,
        fields: &[String],
        mode: WireMode,
    ) -> Result<Value, RpcFault> {
        let meta = self
            .runtime
            .service()
            .get_metainfo(snapshot.id)
            .map_err(RpcFault::from)?;
        let total = snapshot.total_bytes;
        let valid = snapshot.verified_bytes.min(total);
        let mut values = BTreeMap::from([
            ("id", json!(self.ids.id_for(snapshot.id)?)),
            ("hash_string", json!(hex(&snapshot.info_hash))),
            ("name", json!(snapshot.name)),
            ("status", json!(transmission_status(snapshot.status))),
            ("total_size", json!(total)),
            ("have_valid", json!(valid)),
            (
                "percent_done",
                json!(if total == 0 {
                    0.0
                } else {
                    valid as f64 / total as f64
                }),
            ),
            ("downloaded_ever", json!(snapshot.downloaded_bytes)),
            ("uploaded_ever", json!(snapshot.uploaded_bytes)),
            (
                "is_finished",
                json!(snapshot.status == TorrentStatus::Completed || (total > 0 && valid == total)),
            ),
            ("left_until_done", json!(total - valid)),
            (
                "metadata_percent_complete",
                json!(if meta.is_some() { 1.0 } else { 0.0 }),
            ),
            (
                "download_limit",
                json!(
                    snapshot
                        .download_limit
                        .map(|value| value / 1000)
                        .unwrap_or(0)
                ),
            ),
            ("download_limited", json!(snapshot.download_limit.is_some())),
            (
                "upload_limit",
                json!(snapshot.upload_limit.map(|value| value / 1000).unwrap_or(0)),
            ),
            ("upload_limited", json!(snapshot.upload_limit.is_some())),
        ]);
        if fields.iter().any(|field| field == "files")
            && let Some(meta) = meta.as_ref()
        {
            values.insert("files", json!(project_files(meta, snapshot)));
        }
        if let Some(meta) = meta.as_ref() {
            let size_when_done: u64 = meta
                .files
                .iter()
                .enumerate()
                .filter_map(|(index, file)| {
                    (snapshot.file_priorities.get(index) != Some(&FilePriority::Low))
                        .then_some(file.length)
                })
                .sum();
            values.insert("size_when_done", json!(size_when_done));
        }
        if snapshot.downloaded_bytes > 0 {
            values.insert(
                "upload_ratio",
                json!(snapshot.uploaded_bytes as f64 / snapshot.downloaded_bytes as f64),
            );
        }
        let mut output = Map::new();
        for field in fields {
            if let Some(value) = values.get(field.as_str()) {
                output.insert(wire::project_key(mode, field), value.clone());
            }
        }
        Ok(Value::Object(output))
    }

    fn select_torrents(&self, ids: Option<&Value>) -> Result<Vec<TorrentSnapshot>, RpcFault> {
        let all = self.runtime.service().list().map_err(RpcFault::from)?;
        let Some(ids) = ids else {
            return Ok(all);
        };
        let ids = match ids {
            Value::Number(number) => vec![number.as_i64().ok_or(RpcFault::InvalidParams)?],
            Value::String(hash) => vec![self.id_for_query(hash, &all)?],
            Value::Array(values) if values.len() <= MAX_TORRENTS_PER_REQUEST => values
                .iter()
                .map(|value| match value {
                    Value::Number(number) => number.as_i64().ok_or(RpcFault::InvalidParams),
                    Value::String(hash) => self.id_for_query(hash, &all),
                    _ => Err(RpcFault::InvalidParams),
                })
                .collect::<Result<Vec<_>, _>>()?,
            _ => return Err(RpcFault::InvalidParams),
        };
        if ids.len() > MAX_TORRENTS_PER_REQUEST {
            return Err(RpcFault::InvalidParams);
        }
        let mut selected = Vec::with_capacity(ids.len());
        for rpc_id in ids {
            let id = self.ids.torrent_for(rpc_id)?.ok_or(RpcFault::NotFound)?;
            selected.push(self.runtime.service().get(id).map_err(RpcFault::from)?);
        }
        Ok(selected)
    }

    fn id_for_query(&self, value: &str, all: &[TorrentSnapshot]) -> Result<i64, RpcFault> {
        let bytes = parse_hash(value).ok_or(RpcFault::InvalidParams)?;
        let snapshot = all
            .iter()
            .find(|item| item.info_hash == bytes)
            .ok_or(RpcFault::NotFound)?;
        self.ids.id_for(snapshot.id).map_err(RpcFault::from)
    }

    fn torrent_add(
        &self,
        mode: WireMode,
        params: &BTreeMap<String, Value>,
    ) -> Result<Value, RpcFault> {
        reject_unknown(params, &["metainfo", "filename", "paused"])?;
        if params.contains_key("metainfo") == params.contains_key("filename") {
            return Err(RpcFault::InvalidParams);
        }
        let paused = params
            .get("paused")
            .map(as_bool)
            .transpose()?
            .unwrap_or(false);
        let (id, duplicate) = if let Some(value) = params.get("metainfo") {
            let encoded = value.as_str().ok_or(RpcFault::InvalidParams)?;
            if encoded.len() > wire::MAX_REQUEST_BYTES {
                return Err(RpcFault::InvalidParams);
            }
            let bytes = STANDARD
                .decode(encoded)
                .map_err(|_| RpcFault::InvalidParams)?;
            let meta =
                metainfo::parse(&bytes, Default::default()).map_err(|_| RpcFault::InvalidParams)?;
            if let Some(id) = self
                .runtime
                .service()
                .find_by_hash(meta.info_hash)
                .map_err(RpcFault::from)?
            {
                (id, true)
            } else {
                (
                    self.runtime.add_metainfo(&bytes).map_err(RpcFault::from)?,
                    false,
                )
            }
        } else if let Some(value) = params.get("filename") {
            let filename = value.as_str().ok_or(RpcFault::InvalidParams)?;
            if !filename.starts_with("magnet:") {
                return Err(RpcFault::Unsupported);
            }
            let parsed =
                magnet::parse(filename, Default::default()).map_err(|_| RpcFault::InvalidParams)?;
            if let Some(id) = self
                .runtime
                .service()
                .find_by_hash(parsed.info_hash)
                .map_err(RpcFault::from)?
            {
                (id, true)
            } else {
                (
                    self.runtime.add_magnet(filename).map_err(RpcFault::from)?,
                    false,
                )
            }
        } else {
            return Err(RpcFault::InvalidParams);
        };
        let rpc_id = self.ids.id_for(id)?;
        if !paused && !duplicate {
            self.runtime
                .command(TorrentCommand::Start(id))
                .map_err(RpcFault::from)?;
        }
        let snapshot = self.runtime.service().get(id).map_err(RpcFault::from)?;
        let member = if duplicate {
            "torrent_duplicate"
        } else {
            "torrent_added"
        };
        let member = wire::project_key_for_member(mode, member);
        Ok(
            json!({member: {"id": rpc_id, "hash_string": hex(&snapshot.info_hash), "name": snapshot.name}}),
        )
    }

    fn torrent_set(&self, params: &BTreeMap<String, Value>) -> Result<Value, RpcFault> {
        reject_unknown(
            params,
            &[
                "ids",
                "download_limit",
                "download_limited",
                "upload_limit",
                "upload_limited",
                "files_wanted",
                "files_unwanted",
                "priority_high",
                "priority_low",
                "priority_normal",
            ],
        )?;
        let ids = self.select_required_ids(params.get("ids"))?;
        let prepared = ids
            .into_iter()
            .map(|snapshot| {
                let mut download = snapshot.download_limit;
                let mut upload = snapshot.upload_limit;
                apply_limit(params, "download_limit", "download_limited", &mut download)?;
                apply_limit(params, "upload_limit", "upload_limited", &mut upload)?;
                let priorities = priority_updates(params, &snapshot)?;
                Ok::<_, RpcFault>((
                    snapshot.id,
                    snapshot.download_limit,
                    snapshot.upload_limit,
                    download,
                    upload,
                    priorities,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (id, old_download, old_upload, download, upload, priorities) in prepared {
            if download != old_download || upload != old_upload {
                self.runtime
                    .command(TorrentCommand::SetLimits {
                        id,
                        download_bytes_per_second: download,
                        upload_bytes_per_second: upload,
                    })
                    .map_err(RpcFault::from)?;
            }
            if !priorities.is_empty() {
                self.runtime
                    .command(TorrentCommand::SetFilePriorities {
                        id,
                        updates: priorities,
                    })
                    .map_err(RpcFault::from)?;
            }
        }
        Ok(json!({}))
    }

    fn torrent_action(
        &self,
        params: &BTreeMap<String, Value>,
        action: TorrentAction,
    ) -> Result<Value, RpcFault> {
        reject_unknown(params, &["ids"])?;
        let selected = self.select_required_ids(params.get("ids"))?;
        if matches!(action, TorrentAction::Verify) {
            let handle = tokio::runtime::Handle::try_current().map_err(|_| RpcFault::Service)?;
            let total = selected.len();
            if total > MAX_VERIFY_JOBS {
                return Err(RpcFault::InvalidParams);
            }
            let previous = self.verify_jobs.fetch_add(total, Ordering::AcqRel);
            if previous.saturating_add(total) > MAX_VERIFY_JOBS {
                self.verify_jobs.fetch_sub(total, Ordering::AcqRel);
                return Err(RpcFault::Service);
            }
            for (scheduled, snapshot) in selected.into_iter().enumerate() {
                if let Err(error) = self.runtime.command(TorrentCommand::Verify(snapshot.id)) {
                    self.verify_jobs
                        .fetch_sub(total.saturating_sub(scheduled), Ordering::AcqRel);
                    return Err(RpcFault::from(error));
                }
                let runtime = Arc::clone(&self.runtime);
                let jobs = Arc::clone(&self.verify_jobs);
                handle.spawn_blocking(move || {
                    let result = runtime
                        .service()
                        .verify_and_recover(snapshot.id, &Cancellation::default());
                    if result.is_ok() {
                        let _ = runtime.refresh_metainfo(snapshot.id);
                    }
                    jobs.fetch_sub(1, Ordering::AcqRel);
                });
            }
            return Ok(json!({}));
        }
        for snapshot in selected {
            let command = match action {
                TorrentAction::Start => TorrentCommand::Start(snapshot.id),
                TorrentAction::Stop => TorrentCommand::Stop(snapshot.id),
                TorrentAction::Verify => unreachable!(),
                TorrentAction::Reannounce => TorrentCommand::Reannounce(snapshot.id),
            };
            self.runtime.command(command).map_err(RpcFault::from)?;
        }
        Ok(json!({}))
    }

    fn torrent_remove(&self, params: &BTreeMap<String, Value>) -> Result<Value, RpcFault> {
        reject_unknown(params, &["ids", "delete_local_data"])?;
        let delete_data = params
            .get("delete_local_data")
            .map(as_bool)
            .transpose()?
            .unwrap_or(false);
        let mut removed = Vec::new();
        for snapshot in self.select_required_ids(params.get("ids"))? {
            removed.push(json!({"id": self.ids.id_for(snapshot.id)?, "hash_string": hex(&snapshot.info_hash)}));
            self.runtime
                .command(TorrentCommand::Remove {
                    id: snapshot.id,
                    delete_data,
                })
                .map_err(RpcFault::from)?;
        }
        Ok(json!({"removed": removed}))
    }

    fn free_space(&self, params: &BTreeMap<String, Value>) -> Result<Value, RpcFault> {
        reject_unknown(params, &["path"])?;
        if let Some(path) = params.get("path") {
            let path = path.as_str().ok_or(RpcFault::InvalidParams)?;
            if Path::new(path) != self.data_root {
                return Err(RpcFault::Unsupported);
            }
        }
        let bytes = available_space(&self.data_root).map_err(|_| RpcFault::Service)?;
        Ok(json!({"size_bytes": bytes}))
    }

    fn select_required_ids(&self, ids: Option<&Value>) -> Result<Vec<TorrentSnapshot>, RpcFault> {
        if ids.is_none() {
            return Err(RpcFault::InvalidParams);
        }
        self.select_torrents(ids)
    }
}

#[derive(Clone, Copy)]
enum TorrentAction {
    Start,
    Stop,
    Verify,
    Reannounce,
}

fn is_mutating(method: &str) -> bool {
    !matches!(
        method,
        "session_get" | "session_stats" | "torrent_get" | "free_space"
    )
}

fn canonical_params(params: &BTreeMap<String, Value>) -> Result<BTreeMap<String, Value>, RpcFault> {
    let mut output = BTreeMap::new();
    for (key, value) in params {
        let canonical = match key.as_str() {
            "download-dir" | "download_dir" => "download_dir",
            "download-limit" | "downloadLimit" => "download_limit",
            "download-limited" | "downloadLimited" => "download_limited",
            "upload-limit" | "uploadLimit" => "upload_limit",
            "upload-limited" | "uploadLimited" => "upload_limited",
            "delete-local-data" => "delete_local_data",
            "filesWanted" => "files_wanted",
            "filesUnwanted" => "files_unwanted",
            "priorityHigh" => "priority_high",
            "priorityLow" => "priority_low",
            "priorityNormal" => "priority_normal",
            other => other,
        };
        if output.insert(canonical.to_owned(), value.clone()).is_some() {
            return Err(RpcFault::InvalidParams);
        }
    }
    Ok(output)
}

fn reject_unknown(params: &BTreeMap<String, Value>, known: &[&str]) -> Result<(), RpcFault> {
    if params.keys().any(|key| !known.contains(&key.as_str())) {
        Err(RpcFault::InvalidParams)
    } else {
        Ok(())
    }
}

fn fields(value: Option<&Value>, mode: WireMode) -> Result<Vec<String>, RpcFault> {
    let Some(Value::Array(values)) = value else {
        return if value.is_none() {
            Ok(vec![
                "id",
                "hash_string",
                "name",
                "status",
                "total_size",
                "have_valid",
                "percent_done",
                "downloaded_ever",
                "uploaded_ever",
                "is_finished",
                "left_until_done",
                "metadata_percent_complete",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect())
        } else {
            Err(RpcFault::InvalidParams)
        };
    };
    if values.len() > MAX_FIELDS {
        return Err(RpcFault::InvalidParams);
    }
    let mut output = Vec::with_capacity(values.len());
    let mut seen = BTreeSet::new();
    for value in values {
        let Value::String(name) = value else {
            return Err(RpcFault::InvalidParams);
        };
        let field = wire::field_name(mode, name).map_err(|_| RpcFault::Unsupported)?;
        if field == "argument" || !seen.insert(field) {
            return Err(RpcFault::InvalidParams);
        }
        output.push(field.to_owned());
    }
    Ok(output)
}

fn project_fields(
    mode: WireMode,
    values: &BTreeMap<&str, Value>,
    selected: Option<&Value>,
) -> Result<Value, RpcFault> {
    let selected = if let Some(Value::Array(fields)) = selected {
        fields
            .iter()
            .map(|field| {
                let name = field.as_str().ok_or(RpcFault::InvalidParams)?;
                let canonical = wire::field_name(mode, name).map_err(|_| RpcFault::Unsupported)?;
                if canonical == "argument" {
                    return Err(RpcFault::InvalidParams);
                }
                Ok(canonical)
            })
            .collect::<Result<Vec<_>, _>>()?
    } else if selected.is_none() {
        values.keys().copied().collect()
    } else {
        return Err(RpcFault::InvalidParams);
    };
    if selected.len() > MAX_FIELDS {
        return Err(RpcFault::InvalidParams);
    }
    let mut output = Map::new();
    for field in selected {
        let value = values.get(field).ok_or(RpcFault::Unsupported)?;
        output.insert(wire::project_key(mode, field), value.clone());
    }
    Ok(Value::Object(output))
}

fn project_keys(mode: WireMode, values: &BTreeMap<&str, Value>) -> Value {
    Value::Object(
        values
            .iter()
            .map(|(key, value)| (wire::project_key(mode, key), value.clone()))
            .collect(),
    )
}

fn apply_limit(
    params: &BTreeMap<String, Value>,
    limit_key: &str,
    enabled_key: &str,
    target: &mut Option<u64>,
) -> Result<(), RpcFault> {
    let enabled = params.get(enabled_key).map(as_bool).transpose()?;
    let limit = params.get(limit_key).map(as_u64).transpose()?;
    match (enabled, limit) {
        (Some(false), _) => *target = None,
        (Some(true), Some(value)) => {
            *target = Some(value.checked_mul(1000).ok_or(RpcFault::InvalidParams)?)
        }
        (Some(true), None) if target.is_some() => {}
        (Some(true), None) => return Err(RpcFault::InvalidParams),
        (None, Some(value)) => {
            *target = Some(value.checked_mul(1000).ok_or(RpcFault::InvalidParams)?)
        }
        (None, None) => {}
    }
    Ok(())
}

fn priority_updates(
    params: &BTreeMap<String, Value>,
    snapshot: &TorrentSnapshot,
) -> Result<Vec<(u32, FilePriority)>, RpcFault> {
    if [
        "files_wanted",
        "files_unwanted",
        "priority_high",
        "priority_low",
        "priority_normal",
    ]
    .iter()
    .all(|key| !params.contains_key(*key))
    {
        return Ok(Vec::new());
    }
    let mut updates = BTreeMap::new();
    for (key, priority) in [
        ("files_wanted", FilePriority::Normal),
        ("files_unwanted", FilePriority::Low),
        ("priority_high", FilePriority::High),
        ("priority_low", FilePriority::Low),
        ("priority_normal", FilePriority::Normal),
    ] {
        if let Some(value) = params.get(key) {
            let values = value
                .as_array()
                .filter(|values| values.len() <= 100_000)
                .ok_or(RpcFault::InvalidParams)?;
            for value in values {
                let index = u32::try_from(value.as_u64().ok_or(RpcFault::InvalidParams)?)
                    .map_err(|_| RpcFault::InvalidParams)?;
                if index as usize >= snapshot.file_priorities.len()
                    || updates.insert(index, priority).is_some()
                {
                    return Err(RpcFault::InvalidParams);
                }
            }
        }
    }
    Ok(updates.into_iter().collect())
}

fn project_files(meta: &i2pr_tc_core::TorrentMeta, snapshot: &TorrentSnapshot) -> Vec<Value> {
    let mut file_offset = 0u64;
    meta.files.iter().enumerate().map(|(file_index, file)| {
        let file_end = file_offset.saturating_add(file.length);
        let mut completed = 0u64;
        let first_piece = file_offset / meta.piece_length as u64;
        let end_piece = file_end.div_ceil(meta.piece_length as u64);
        for piece in first_piece..end_piece {
            if snapshot.verified_pieces.get(piece as usize).copied().unwrap_or(false) {
                let piece_start = piece * meta.piece_length as u64;
                completed += file_end.min(piece_start + meta.piece_length as u64).saturating_sub(file_offset.max(piece_start));
            }
        }
        file_offset = file_end;
        let name = file.path.join("/");
        let priority = snapshot.file_priorities.get(file_index).copied().unwrap_or(FilePriority::Normal);
        let priority = match priority { FilePriority::High => 1, FilePriority::Normal => 0, FilePriority::Low => -1 };
        json!({"name": name, "length": file.length, "bytes_completed": completed, "priority": priority})
    }).collect()
}

fn transmission_status(status: TorrentStatus) -> u8 {
    match status {
        TorrentStatus::Stopped | TorrentStatus::Error => 0,
        TorrentStatus::Checking => 2,
        TorrentStatus::Starting => 3,
        TorrentStatus::Running => 4,
        TorrentStatus::Completed => 6,
    }
}

fn as_bool(value: &Value) -> Result<bool, RpcFault> {
    value.as_bool().ok_or(RpcFault::InvalidParams)
}
fn as_u64(value: &Value) -> Result<u64, RpcFault> {
    value.as_u64().ok_or(RpcFault::InvalidParams)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn parse_hash(value: &str) -> Option<[u8; 20]> {
    if value.len() != 40 {
        return None;
    }
    let mut bytes = [0; 20];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn root() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "i2pr-tc-rpc-adapter-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn metainfo() -> Vec<u8> {
        let mut bytes = b"d4:infod6:lengthi4e4:name1:x12:piece lengthi4e6:pieces20:".to_vec();
        bytes.extend([0u8; 20]);
        bytes.extend_from_slice(b"ee");
        bytes
    }

    fn adapter(root: &Path) -> TransmissionAdapter {
        let data_root = root.join("payload");
        std::fs::create_dir_all(&data_root).unwrap();
        let runtime = TorrentRuntime::open(
            root.join("catalog"),
            &i2pr_tc_storage::Cancellation::default(),
            16,
            32,
            8,
        )
        .unwrap();
        TransmissionAdapter::new(
            Arc::new(runtime),
            Arc::new(RpcIdStore::open(root.join("rpc-ids.json")).unwrap()),
            data_root,
        )
        .unwrap()
    }

    fn invoke(adapter: &TransmissionAdapter, request: Value) -> RpcResponse {
        adapter
            .handle(&serde_json::to_vec(&request).unwrap())
            .unwrap()
    }

    #[test]
    fn current_and_legacy_requests_share_native_mutations_and_truthful_fields() {
        let root = root();
        let adapter = adapter(&root);
        let encoded_metainfo = STANDARD.encode(metainfo());
        let add = invoke(
            &adapter,
            json!({
                "jsonrpc":"2.0", "method":"torrent_add",
            "params":{"metainfo":encoded_metainfo,"paused":true}, "id":1
            }),
        );
        let value = add.result.unwrap();
        let torrent = value.get("torrent_added").unwrap();
        let id = torrent["id"].as_i64().unwrap();
        let hash = torrent["hash_string"].as_str().unwrap().to_owned();

        let duplicate = invoke(
            &adapter,
            json!({
                "jsonrpc":"2.0", "method":"torrent_add",
                "params":{"metainfo":STANDARD.encode(metainfo())}, "id":2
            }),
        );
        assert!(duplicate.result.unwrap().get("torrent_duplicate").is_some());

        let set = invoke(
            &adapter,
            json!({
                "jsonrpc":"2.0", "method":"torrent_set",
                "params":{"ids":id,"download_limit":12,"download_limited":true,"priority_high":[0]}, "id":3
            }),
        );
        assert!(set.result.is_ok());
        let get = invoke(
            &adapter,
            json!({
                "jsonrpc":"2.0", "method":"torrent_get",
                "params":{"ids":hash,"fields":["id","hash_string","download_limit","download_limited","files"]}, "id":4
            }),
        );
        let torrent = &get.result.unwrap()["torrents"][0];
        assert_eq!(torrent["id"], id);
        assert_eq!(torrent["download_limit"], 12);
        assert_eq!(torrent["download_limited"], true);
        assert_eq!(torrent["files"][0]["priority"], 1);

        let legacy = adapter
            .handle(br#"{"method":"torrent-get","arguments":{"ids":1,"fields":["hashString"]},"tag":5}"#)
            .unwrap();
        assert_eq!(legacy.mode, WireMode::Legacy);
        let legacy_body: Value =
            serde_json::from_slice(&crate::adapter::encode_response(&legacy).unwrap()).unwrap();
        assert_eq!(legacy_body["result"], "success");
        assert_eq!(legacy_body["arguments"]["torrents"][0]["hashString"], hash);
        drop(adapter);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn malformed_or_unsupported_mutations_do_not_change_native_state() {
        let root = root();
        let adapter = adapter(&root);
        let id = adapter.runtime.add_metainfo(&metainfo()).unwrap();
        let before = adapter.runtime.service().get(id).unwrap();
        let bad = adapter.handle(br#"{"jsonrpc":"2.0","method":"torrent_set","params":{"ids":1,"labels":["no"]},"id":1}"#).unwrap();
        assert_eq!(bad.result, Err(RpcFault::InvalidParams));
        let duplicate_key = adapter.handle(
            br#"{"jsonrpc":"2.0","method":"torrent_set","params":{"ids":1,"ids":2},"id":1}"#,
        );
        assert!(duplicate_key.is_err());
        let after = adapter.runtime.service().get(id).unwrap();
        assert_eq!(after.download_limit, before.download_limit);
        assert_eq!(after.file_priorities, before.file_priorities);
        drop(adapter);
        let _ = std::fs::remove_dir_all(root);
    }
}
