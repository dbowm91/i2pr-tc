//! Bounded BitTorrent extension protocol primitives without network behavior.
use crate::bencode::{self, Value};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExtensionError {
    #[error("invalid extension message")]
    Invalid,
    #[error("extension message exceeds a configured bound")]
    Limit,
    #[error("unsupported metadata message type")]
    Unsupported,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UtMetadata {
    Request {
        piece: u32,
    },
    Data {
        piece: u32,
        total_size: u32,
        block: Vec<u8>,
    },
    Reject {
        piece: u32,
    },
}
#[derive(Clone, Copy, Debug)]
pub struct MetadataLimits {
    pub total_size: u32,
    pub block_size: usize,
    pub encoded_header: usize,
}
impl Default for MetadataLimits {
    fn default() -> Self {
        Self {
            total_size: 8 * 1024 * 1024,
            block_size: 16 * 1024,
            encoded_header: 1024,
        }
    }
}

pub fn parse_ut_metadata(
    payload: &[u8],
    limits: MetadataLimits,
) -> Result<UtMetadata, ExtensionError> {
    let max_payload = limits
        .encoded_header
        .checked_add(limits.block_size)
        .ok_or(ExtensionError::Limit)?;
    if limits.block_size == 0 || payload.len() > max_payload {
        return Err(ExtensionError::Limit);
    }
    let header_cap = limits
        .encoded_header
        .checked_add(1)
        .ok_or(ExtensionError::Limit)?
        .min(payload.len());
    let header_input = &payload[..header_cap];
    let (header, used) = bencode::parse_prefix(
        header_input,
        bencode::Limits {
            input: header_input.len(),
            ..Default::default()
        },
    )
    .map_err(|_| ExtensionError::Invalid)?;
    if used > limits.encoded_header {
        return Err(ExtensionError::Limit);
    }
    let msg_type = integer(&header, payload, b"msg_type")?;
    let piece = integer(&header, payload, b"piece")?;
    if piece < 0 || piece as u64 > u32::MAX as u64 {
        return Err(ExtensionError::Invalid);
    }
    let piece = piece as u32;
    match msg_type {
        0 => {
            if used == payload.len() {
                Ok(UtMetadata::Request { piece })
            } else {
                Err(ExtensionError::Invalid)
            }
        }
        2 => {
            if used == payload.len() {
                Ok(UtMetadata::Reject { piece })
            } else {
                Err(ExtensionError::Invalid)
            }
        }
        1 => {
            let total = integer(&header, payload, b"total_size")?;
            if total <= 0 || total as u64 > limits.total_size as u64 {
                return Err(ExtensionError::Limit);
            }
            let block = &payload[used..];
            if block.is_empty() || block.len() > limits.block_size {
                return Err(ExtensionError::Limit);
            }
            let start = (piece as u64)
                .checked_mul(limits.block_size as u64)
                .ok_or(ExtensionError::Limit)?;
            if start >= total as u64 {
                return Err(ExtensionError::Invalid);
            }
            let expected = (total as u64 - start).min(limits.block_size as u64);
            if block.len() as u64 != expected {
                return Err(ExtensionError::Invalid);
            }
            Ok(UtMetadata::Data {
                piece,
                total_size: total as u32,
                block: block.to_vec(),
            })
        }
        _ => Err(ExtensionError::Unsupported),
    }
}

/// Parses the extension handshake's `m` dictionary. Unknown extensions are
/// retained as IDs for callers to ignore; duplicate names fail closed.
pub fn parse_extension_map(
    input: &[u8],
    max_entries: usize,
) -> Result<BTreeMap<String, u8>, ExtensionError> {
    let root = bencode::parse(
        input,
        bencode::Limits {
            input: 4096,
            items: max_entries.saturating_mul(2).saturating_add(16),
            ..Default::default()
        },
    )
    .map_err(|_| ExtensionError::Invalid)?;
    let m = bencode::dict_get(&root, input, b"m").ok_or(ExtensionError::Invalid)?;
    let Value::Dict(entries) = m else {
        return Err(ExtensionError::Invalid);
    };
    if entries.len() > max_entries {
        return Err(ExtensionError::Limit);
    }
    let mut map = BTreeMap::new();
    for (k, v) in entries {
        let name = std::str::from_utf8(&input[k.clone()]).map_err(|_| ExtensionError::Invalid)?;
        let Value::Integer(id) = v else {
            return Err(ExtensionError::Invalid);
        };
        if *id < 0 || *id > 255 || map.insert(name.to_owned(), *id as u8).is_some() {
            return Err(ExtensionError::Invalid);
        }
    }
    Ok(map)
}
fn integer(v: &Value, src: &[u8], key: &[u8]) -> Result<i64, ExtensionError> {
    if let Some(Value::Integer(n)) = bencode::dict_get(v, src, key) {
        Ok(*n)
    } else {
        Err(ExtensionError::Invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metadata_blocks_are_bounded_and_typed() {
        let mut p = b"d8:msg_typei1e5:piecei0e10:total_sizei3ee".to_vec();
        p.extend_from_slice(b"abc");
        assert_eq!(
            parse_ut_metadata(&p, MetadataLimits::default()).unwrap(),
            UtMetadata::Data {
                piece: 0,
                total_size: 3,
                block: b"abc".to_vec()
            }
        );
        let bad = b"d8:msg_typei0e5:piecei0eeunrequested";
        assert_eq!(
            parse_ut_metadata(bad, MetadataLimits::default()),
            Err(ExtensionError::Invalid)
        );
        let mut short_nonfinal = b"d8:msg_typei1e5:piecei0e10:total_sizei6ee".to_vec();
        short_nonfinal.extend_from_slice(b"abc");
        assert_eq!(
            parse_ut_metadata(
                &short_nonfinal,
                MetadataLimits {
                    total_size: 16,
                    block_size: 4,
                    encoded_header: 128,
                }
            ),
            Err(ExtensionError::Invalid)
        );
        let mut large_valid = b"d8:msg_typei1e5:piecei0e10:total_sizei1500ee".to_vec();
        large_valid.extend(vec![0x5a; 1500]);
        assert!(matches!(
            parse_ut_metadata(&large_valid, MetadataLimits::default()),
            Ok(UtMetadata::Data { block, .. }) if block.len() == 1500
        ));
    }
}
