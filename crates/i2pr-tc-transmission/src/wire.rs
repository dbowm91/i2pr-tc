//! Strict current JSON-RPC and legacy Transmission envelope parsing.
use serde::{
    de::{MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::{Map, Number, Value};
use std::{collections::BTreeMap, fmt};
use thiserror::Error;

pub const MAX_REQUEST_BYTES: usize = 1024 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_ARRAY_ITEMS: usize = 4096;
const MAX_OBJECT_MEMBERS: usize = 1024;
const MAX_STRING_BYTES: usize = 64 * 1024;
const MAX_JSON_NODES: usize = 100_000;
const MAX_JSON_DEPTH: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireMode {
    JsonRpc,
    Legacy,
}

#[derive(Clone, Debug)]
pub struct RpcRequest {
    pub mode: WireMode,
    pub method: String,
    pub params: BTreeMap<String, Value>,
    pub id: Option<Value>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WireError {
    #[error("request is malformed or violates a field bound")]
    Invalid,
    #[error("request or response exceeds its byte bound")]
    Limit,
}

struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueVisitor)
    }
}

struct UniqueVisitor;

impl<'de> Visitor<'de> for UniqueVisitor {
    type Value = UniqueValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded JSON without duplicate object keys")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }
    fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        self.visit_unit()
    }
    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Bool(value)))
    }
    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(value.into())))
    }
    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(value.into())))
    }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
        Number::from_f64(value)
            .map(Value::Number)
            .map(UniqueValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        if value.len() > MAX_STRING_BYTES {
            return Err(serde::de::Error::custom("string limit"));
        }
        Ok(UniqueValue(Value::String(value.to_owned())))
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        if value.len() > MAX_STRING_BYTES {
            return Err(serde::de::Error::custom("string limit"));
        }
        Ok(UniqueValue(Value::String(value)))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(UniqueValue(value)) = sequence.next_element()? {
            if values.len() >= MAX_ARRAY_ITEMS {
                return Err(serde::de::Error::custom("array limit"));
            }
            values.push(value);
        }
        Ok(UniqueValue(Value::Array(values)))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let mut values = Map::new();
        while let Some((key, UniqueValue(value))) = access.next_entry::<String, UniqueValue>()? {
            if key.len() > MAX_STRING_BYTES || values.len() >= MAX_OBJECT_MEMBERS {
                return Err(serde::de::Error::custom("object limit"));
            }
            if values.insert(key, value).is_some() {
                return Err(serde::de::Error::custom("duplicate object key"));
            }
        }
        Ok(UniqueValue(Value::Object(values)))
    }
}

pub fn parse_request(bytes: &[u8]) -> Result<RpcRequest, WireError> {
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(WireError::Limit);
    }
    let UniqueValue(root) = serde_json::from_slice(bytes).map_err(|_| WireError::Invalid)?;
    enforce_node_limit(&root)?;
    let Value::Object(mut object) = root else {
        return Err(WireError::Invalid);
    };
    let mode = if object.contains_key("jsonrpc") {
        WireMode::JsonRpc
    } else {
        WireMode::Legacy
    };
    let (method, params, id) = match mode {
        WireMode::JsonRpc => {
            if object
                .keys()
                .any(|key| !matches!(key.as_str(), "jsonrpc" | "method" | "params" | "id"))
                || object.remove("jsonrpc") != Some(Value::String("2.0".into()))
            {
                return Err(WireError::Invalid);
            }
            let method = take_string(&mut object, "method")?;
            let params = object
                .remove("params")
                .unwrap_or_else(|| Value::Object(Map::new()));
            let id = object.remove("id");
            if id.as_ref().is_some_and(|id| !valid_rpc_id(id)) {
                return Err(WireError::Invalid);
            }
            (method, params, id)
        }
        WireMode::Legacy => {
            if object
                .keys()
                .any(|key| !matches!(key.as_str(), "method" | "arguments" | "tag"))
            {
                return Err(WireError::Invalid);
            }
            let method = take_string(&mut object, "method")?;
            let params = object
                .remove("arguments")
                .unwrap_or_else(|| Value::Object(Map::new()));
            let id = object.remove("tag");
            if id.as_ref().is_some_and(|id| !valid_rpc_id(id)) {
                return Err(WireError::Invalid);
            }
            (method, params, id)
        }
    };
    if method.is_empty() || method.len() > 128 {
        return Err(WireError::Invalid);
    }
    let Value::Object(params) = params else {
        return Err(WireError::Invalid);
    };
    let params = params.into_iter().collect();
    Ok(RpcRequest {
        mode,
        method,
        params,
        id,
    })
}

fn valid_rpc_id(value: &Value) -> bool {
    match value {
        Value::Null | Value::String(_) => true,
        Value::Number(number) => number.as_i64().is_some(),
        _ => false,
    }
}

fn take_string(object: &mut Map<String, Value>, key: &str) -> Result<String, WireError> {
    match object.remove(key) {
        Some(Value::String(value)) => Ok(value),
        _ => Err(WireError::Invalid),
    }
}

fn enforce_node_limit(root: &Value) -> Result<(), WireError> {
    let mut stack = vec![(root, 0usize)];
    let mut nodes = 0usize;
    while let Some((value, depth)) = stack.pop() {
        nodes += 1;
        if nodes > MAX_JSON_NODES || depth > MAX_JSON_DEPTH {
            return Err(WireError::Limit);
        }
        match value {
            Value::Array(values) => stack.extend(values.iter().map(|value| (value, depth + 1))),
            Value::Object(values) => stack.extend(values.values().map(|value| (value, depth + 1))),
            _ => {}
        }
    }
    Ok(())
}

pub fn canonical_method(mode: WireMode, method: &str) -> Result<&'static str, WireError> {
    match method {
        "session_get" | "session-get" | "sessionGet" => Ok("session_get"),
        "session_stats" | "session-stats" | "sessionStats" => Ok("session_stats"),
        "torrent_get" | "torrent-get" | "torrentGet" => Ok("torrent_get"),
        "torrent_add" | "torrent-add" | "torrentAdd" => Ok("torrent_add"),
        "torrent_set" | "torrent-set" | "torrentSet" => Ok("torrent_set"),
        "torrent_start" | "torrent-start" | "torrentStart" => Ok("torrent_start"),
        "torrent_start_now" | "torrent-start-now" | "torrentStartNow" => Ok("torrent_start_now"),
        "torrent_stop" | "torrent-stop" | "torrentStop" => Ok("torrent_stop"),
        "torrent_verify" | "torrent-verify" | "torrentVerify" => Ok("torrent_verify"),
        "torrent_reannounce" | "torrent-reannounce" | "torrentReannounce" => {
            Ok("torrent_reannounce")
        }
        "torrent_remove" | "torrent-remove" | "torrentRemove" => Ok("torrent_remove"),
        "free_space" | "free-space" | "freeSpace" => Ok("free_space"),
        _ if mode == WireMode::Legacy => Err(WireError::Invalid),
        _ => Err(WireError::Invalid),
    }
}

pub fn field_name(mode: WireMode, name: &str) -> Result<&'static str, WireError> {
    match name {
        "id" => Ok("id"),
        "added_date" | "addedDate" => Ok("added_date"),
        "error" => Ok("error"),
        "error_string" | "errorString" => Ok("error_string"),
        "eta" => Ok("eta"),
        "hash_string" | "hashString" => Ok("hash_string"),
        "name" => Ok("name"),
        "version" => Ok("version"),
        "rpc_version_semver" | "rpcVersionSemver" => Ok("rpc_version_semver"),
        "status" => Ok("status"),
        "total_size" | "totalSize" => Ok("total_size"),
        "have_valid" | "haveValid" => Ok("have_valid"),
        "percent_done" | "percentDone" => Ok("percent_done"),
        "downloaded_ever" | "downloadedEver" => Ok("downloaded_ever"),
        "uploaded_ever" | "uploadedEver" => Ok("uploaded_ever"),
        "is_finished" | "isFinished" => Ok("is_finished"),
        "left_until_done" | "leftUntilDone" => Ok("left_until_done"),
        "peers_getting_from_us" | "peersGettingFromUs" => Ok("peers_getting_from_us"),
        "peers_sending_to_us" | "peersSendingToUs" => Ok("peers_sending_to_us"),
        "rate_download" | "rateDownload" => Ok("rate_download"),
        "rate_upload" | "rateUpload" => Ok("rate_upload"),
        "size_when_done" | "sizeWhenDone" => Ok("size_when_done"),
        "upload_ratio" | "uploadRatio" => Ok("upload_ratio"),
        "metadata_percent_complete" | "metadataPercentComplete" => Ok("metadata_percent_complete"),
        "files" => Ok("files"),
        "download_limit" | "downloadLimit" => Ok("download_limit"),
        "download_limited" | "downloadLimited" => Ok("download_limited"),
        "upload_limit" | "uploadLimit" => Ok("upload_limit"),
        "upload_limited" | "uploadLimited" => Ok("upload_limited"),
        "files_wanted" | "filesWanted" => Ok("files_wanted"),
        "files_unwanted" | "filesUnwanted" => Ok("files_unwanted"),
        "priority_high" | "priorityHigh" => Ok("priority_high"),
        "priority_low" | "priorityLow" => Ok("priority_low"),
        "priority_normal" | "priorityNormal" => Ok("priority_normal"),
        "metainfo" | "filename" | "paused" | "delete_local_data" | "delete-local-data" | "ids"
        | "fields" | "path" => Ok("argument"),
        _ if mode == WireMode::Legacy => Err(WireError::Invalid),
        _ => Err(WireError::Invalid),
    }
}

pub fn project_key(mode: WireMode, canonical: &str) -> String {
    if mode == WireMode::JsonRpc {
        canonical.to_owned()
    } else {
        let mut out = String::new();
        let mut upper = false;
        for character in canonical.chars() {
            if character == '_' {
                upper = true;
            } else if upper {
                out.extend(character.to_uppercase());
                upper = false;
            } else {
                out.push(character);
            }
        }
        out
    }
}

pub fn project_key_for_member(mode: WireMode, canonical: &str) -> String {
    if mode == WireMode::Legacy {
        match canonical {
            "torrent_added" => return "torrent-added".into(),
            "torrent_duplicate" => return "torrent-duplicate".into(),
            "size_bytes" => return "size-bytes".into(),
            _ => {}
        }
    }
    project_key(mode, canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_and_legacy_envelopes_share_one_canonical_method() {
        let current = parse_request(br#"{"jsonrpc":"2.0","method":"torrent_get","params":{"fields":["hash_string"]},"id":2}"#).unwrap();
        let legacy = parse_request(
            br#"{"method":"torrent-get","arguments":{"fields":["hashString"]},"tag":2}"#,
        )
        .unwrap();
        assert_eq!(
            canonical_method(current.mode, &current.method),
            Ok("torrent_get")
        );
        assert_eq!(
            canonical_method(legacy.mode, &legacy.method),
            Ok("torrent_get")
        );
        assert_eq!(
            field_name(WireMode::Legacy, "hashString"),
            Ok("hash_string")
        );
    }

    #[test]
    fn rejects_duplicate_unknown_and_over_limit_json() {
        assert!(matches!(
            parse_request(br#"{"jsonrpc":"2.0","method":"session_get","method":"session_stats"}"#),
            Err(WireError::Invalid)
        ));
        assert!(matches!(
            parse_request(br#"{"jsonrpc":"2.0","method":"session_get","evil":1}"#),
            Err(WireError::Invalid)
        ));
        assert!(matches!(
            parse_request(&vec![b' '; MAX_REQUEST_BYTES + 1]),
            Err(WireError::Limit)
        ));
        assert!(matches!(
            parse_request(br#"{"jsonrpc":"2.0","method":"session_get","params":[]}"#),
            Err(WireError::Invalid)
        ));
        let mut nested = "null".to_owned();
        for _ in 0..=MAX_JSON_DEPTH {
            nested = format!("[{nested}]");
        }
        let deep =
            format!(r#"{{"jsonrpc":"2.0","method":"session_get","params":{{"nested":{nested}}}}}"#);
        assert!(matches!(
            parse_request(deep.as_bytes()),
            Err(WireError::Limit)
        ));
    }
}
