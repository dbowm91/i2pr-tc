use crate::bencode::{self, Value};
use sha1::{Digest, Sha1};
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct InfoHashV1(pub [u8; 20]);
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TorrentFile {
    pub path: Vec<String>,
    pub length: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TorrentMeta {
    pub info_hash: InfoHashV1,
    pub name: String,
    pub piece_length: u32,
    pub piece_hashes: Vec<[u8; 20]>,
    pub files: Vec<TorrentFile>,
    pub total_length: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct MetaLimits {
    pub encoded: usize,
    pub files: usize,
    pub path_bytes: usize,
    pub pieces: usize,
    pub total_bytes: u64,
    pub piece_length: u32,
}
impl Default for MetaLimits {
    fn default() -> Self {
        Self {
            encoded: 64 * 1024 * 1024,
            files: 100_000,
            path_bytes: 4096,
            pieces: 4_000_000,
            total_bytes: 1 << 50,
            piece_length: 16 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Error)]
pub enum MetaError {
    #[error(transparent)]
    Bencode(#[from] bencode::Error),
    #[error("invalid torrent metainfo: {0}")]
    Invalid(&'static str),
    #[error("metainfo bound exceeded")]
    Limit,
}

pub fn parse(input: &[u8], limits: MetaLimits) -> Result<TorrentMeta, MetaError> {
    if input.len() > limits.encoded {
        return Err(MetaError::Limit);
    }
    let root = bencode::parse(
        input,
        bencode::Limits {
            input: limits.encoded,
            ..Default::default()
        },
    )?;
    let info =
        bencode::dict_get(&root, input, b"info").ok_or(MetaError::Invalid("missing info"))?;
    let Value::Dict(fields) = info else {
        return Err(MetaError::Invalid("info is not a dictionary"));
    };
    let first_key = fields
        .first()
        .map(|(k, _)| k.start)
        .ok_or(MetaError::Invalid("empty info"))?;
    let mut token_start = first_key
        .checked_sub(1)
        .ok_or(MetaError::Invalid("info key"))?;
    while token_start > 0 && input[token_start - 1].is_ascii_digit() {
        token_start -= 1;
    }
    let info_start = token_start
        .checked_sub(1)
        .ok_or(MetaError::Invalid("info start"))?;
    if input.get(info_start) != Some(&b'd') {
        return Err(MetaError::Invalid("info dictionary start"));
    }
    // Find the matching end marker using the already validated bencode framing.
    let info_end = encoded_value_end(input, info_start)?;
    let mut h = Sha1::new();
    h.update(&input[info_start..info_end]);
    let info_hash = InfoHashV1(h.finalize().into());
    let name = text_field(info, b"name", input)?;
    let pl = int_field(info, b"piece length", input)?;
    if pl <= 0 || pl > limits.piece_length as i64 {
        return Err(MetaError::Invalid("piece length"));
    }
    let p = bencode::bytes(
        bencode::dict_get(info, input, b"pieces").ok_or(MetaError::Invalid("missing pieces"))?,
        input,
    )
    .ok_or(MetaError::Invalid("pieces type"))?;
    if p.len() % 20 != 0 || p.len() / 20 > limits.pieces {
        return Err(MetaError::Limit);
    }
    let piece_hashes = p
        .chunks_exact(20)
        .map(|c| c.try_into().unwrap())
        .collect::<Vec<[u8; 20]>>();
    let files = if let Some(v) = bencode::dict_get(info, input, b"length") {
        let n = as_nonnegative(v).ok_or(MetaError::Invalid("length"))?;
        vec![TorrentFile {
            path: vec![name.clone()],
            length: n,
        }]
    } else {
        let Value::List(entries) =
            bencode::dict_get(info, input, b"files").ok_or(MetaError::Invalid("missing files"))?
        else {
            return Err(MetaError::Invalid("files type"));
        };
        if entries.is_empty() || entries.len() > limits.files {
            return Err(MetaError::Limit);
        }
        entries
            .iter()
            .map(|e| {
                let n = as_nonnegative(
                    bencode::dict_get(e, input, b"length")
                        .ok_or(MetaError::Invalid("file length"))?,
                )
                .ok_or(MetaError::Invalid("file length"))?;
                let Value::List(parts) =
                    bencode::dict_get(e, input, b"path").ok_or(MetaError::Invalid("path"))?
                else {
                    return Err(MetaError::Invalid("path type"));
                };
                if parts.is_empty() || parts.len() > 64 {
                    return Err(MetaError::Limit);
                }
                let mut path = vec![name.clone()];
                for part in parts {
                    let raw =
                        bencode::bytes(part, input).ok_or(MetaError::Invalid("path component"))?;
                    if raw.len() > limits.path_bytes {
                        return Err(MetaError::Limit);
                    }
                    path.push(valid_component(raw)?);
                }
                Ok(TorrentFile { path, length: n })
            })
            .collect::<Result<Vec<_>, MetaError>>()?
    };
    let total_length = files
        .iter()
        .try_fold(0u64, |a, f| a.checked_add(f.length))
        .ok_or(MetaError::Limit)?;
    if total_length > limits.total_bytes {
        return Err(MetaError::Limit);
    }
    let expected_u64 = if total_length == 0 {
        0
    } else {
        (total_length - 1) / (pl as u64) + 1
    };
    let expected = usize::try_from(expected_u64).map_err(|_| MetaError::Limit)?;
    if expected != piece_hashes.len() {
        return Err(MetaError::Invalid("piece count does not match length"));
    }
    Ok(TorrentMeta {
        info_hash,
        name,
        piece_length: pl as u32,
        piece_hashes,
        files,
        total_length,
    })
}
fn encoded_value_end(input: &[u8], start: usize) -> Result<usize, MetaError> {
    let mut i = start;
    let mut depth = 0usize;
    loop {
        let b = *input.get(i).ok_or(MetaError::Invalid("truncated info"))?;
        match b {
            b'l' | b'd' => {
                depth += 1;
                i += 1
            }
            b'e' => {
                depth -= 1;
                i += 1;
                if depth == 0 {
                    return Ok(i);
                }
            }
            b'i' => {
                i += 1;
                while input.get(i) != Some(&b'e') {
                    i += 1;
                }
                i += 1
            }
            b'0'..=b'9' => {
                let mut n = 0usize;
                while input[i].is_ascii_digit() {
                    n = n * 10 + (input[i] - b'0') as usize;
                    i += 1;
                }
                i += 1;
                i = i.checked_add(n).ok_or(MetaError::Limit)?
            }
            _ => return Err(MetaError::Invalid("info framing")),
        }
    }
}
fn as_nonnegative(v: &Value) -> Option<u64> {
    if let Value::Integer(i) = v {
        u64::try_from(*i).ok()
    } else {
        None
    }
}
fn int_field(v: &Value, k: &[u8], src: &[u8]) -> Result<i64, MetaError> {
    if let Value::Integer(n) =
        bencode::dict_get(v, src, k).ok_or(MetaError::Invalid("integer field"))?
    {
        Ok(*n)
    } else {
        Err(MetaError::Invalid("integer field type"))
    }
}
fn text_field(v: &Value, k: &[u8], src: &[u8]) -> Result<String, MetaError> {
    let raw = bencode::bytes(
        bencode::dict_get(v, src, k).ok_or(MetaError::Invalid("name"))?,
        src,
    )
    .ok_or(MetaError::Invalid("name type"))?;
    valid_component(raw)
}
fn valid_component(raw: &[u8]) -> Result<String, MetaError> {
    let s = std::str::from_utf8(raw).map_err(|_| MetaError::Invalid("path is not UTF-8"))?;
    if s.is_empty()
        || s == "."
        || s == ".."
        || s.contains('/')
        || s.contains('\\')
        || s.contains(':')
        || s.contains('\0')
        || s.starts_with('/')
    {
        return Err(MetaError::Invalid("unsafe path component"));
    }
    Ok(s.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hashes_the_original_info_bytes() {
        let mut data = b"d4:infod6:lengthi1e4:name1:x12:piece lengthi1e6:pieces20:".to_vec();
        data.extend([0u8; 20]);
        data.extend_from_slice(b"ee");
        let meta = parse(&data, MetaLimits::default()).unwrap();
        let start = data.windows(7).position(|w| w == b"4:infod").unwrap() + 6;
        let mut h = Sha1::new();
        h.update(&data[start..data.len() - 1]);
        assert_eq!(meta.info_hash.0, <[u8; 20]>::from(h.finalize()));
    }
    #[test]
    fn rejects_unsafe_paths_and_wrong_piece_count() {
        let mut data = b"d4:infod6:lengthi1e4:name3:../12:piece lengthi1e6:pieces20:".to_vec();
        data.extend([0u8; 20]);
        data.extend_from_slice(b"ee");
        assert!(parse(&data, MetaLimits::default()).is_err());
        let mut bad = b"d4:infod6:lengthi2e4:name1:x12:piece lengthi1e6:pieces20:".to_vec();
        bad.extend([0u8; 20]);
        bad.extend_from_slice(b"ee");
        assert!(parse(&bad, MetaLimits::default()).is_err());
    }
}
