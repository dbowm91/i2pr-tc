use crate::bencode::{self, Value};
use sha1::{Digest, Sha1};
use std::collections::BTreeSet;
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
    pub trackers: Vec<String>,
}
#[derive(Clone, Copy, Debug)]
pub struct MetaLimits {
    pub encoded: usize,
    pub files: usize,
    pub path_bytes: usize,
    pub pieces: usize,
    pub total_bytes: u64,
    pub piece_length: u32,
    pub announce_count: usize,
    pub announce_bytes: usize,
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
            announce_count: 64,
            announce_bytes: 2048,
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
    let trackers = parse_trackers(&root, input, limits)?;
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
    validate_file_layout(&files)?;
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
        trackers,
    })
}

fn parse_trackers(
    root: &Value,
    input: &[u8],
    limits: MetaLimits,
) -> Result<Vec<String>, MetaError> {
    let mut trackers = Vec::new();
    if let Some(announce) = bencode::dict_get(root, input, b"announce") {
        push_tracker(announce, input, limits, &mut trackers)?;
    }
    if let Some(tiers) = bencode::dict_get(root, input, b"announce-list") {
        let Value::List(tiers) = tiers else {
            return Err(MetaError::Invalid("announce-list type"));
        };
        for tier in tiers {
            let Value::List(entries) = tier else {
                return Err(MetaError::Invalid("announce tier type"));
            };
            for entry in entries {
                push_tracker(entry, input, limits, &mut trackers)?;
            }
        }
    }
    Ok(trackers)
}

fn push_tracker(
    value: &Value,
    input: &[u8],
    limits: MetaLimits,
    trackers: &mut Vec<String>,
) -> Result<(), MetaError> {
    let bytes = bencode::bytes(value, input).ok_or(MetaError::Invalid("announce URL type"))?;
    if bytes.is_empty() || bytes.len() > limits.announce_bytes {
        return Err(MetaError::Limit);
    }
    let tracker = std::str::from_utf8(bytes)
        .map_err(|_| MetaError::Invalid("announce URL encoding"))?
        .to_owned();
    if !trackers.contains(&tracker) {
        if trackers.len() >= limits.announce_count {
            return Err(MetaError::Limit);
        }
        trackers.push(tracker);
    }
    Ok(())
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
        || s.len() > 255
        || s == "."
        || s == ".."
        || s.ends_with('.')
        || s.ends_with(' ')
        || s.contains('/')
        || s.contains('\\')
        || s.contains(':')
        || s.contains('\0')
        || s.starts_with('/')
        || s.chars().any(|c| c.is_control() || "<>|?*\"".contains(c))
    {
        return Err(MetaError::Invalid("unsafe path component"));
    }
    let stem = s.split('.').next().unwrap_or("").to_ascii_uppercase();
    if matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ((stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.len() == 4
            && matches!(stem.as_bytes()[3], b'1'..=b'9'))
    {
        return Err(MetaError::Invalid("reserved path component"));
    }
    Ok(s.to_owned())
}

fn validate_file_layout(files: &[TorrentFile]) -> Result<(), MetaError> {
    let mut paths = BTreeSet::new();
    let mut portable_paths = BTreeSet::new();
    for file in files {
        if !paths.insert(file.path.clone()) {
            return Err(MetaError::Invalid("duplicate file path"));
        }
        let portable: Vec<String> = file.path.iter().map(|part| part.to_lowercase()).collect();
        if !portable_paths.insert(portable.clone()) {
            return Err(MetaError::Invalid("case-colliding file path"));
        }
        for depth in 1..file.path.len() {
            if paths.contains(&file.path[..depth]) || portable_paths.contains(&portable[..depth]) {
                return Err(MetaError::Invalid("file path is also a directory"));
            }
        }
    }
    for path in &paths {
        if paths
            .range(path.clone()..)
            .nth(1)
            .is_some_and(|next| next.starts_with(path))
        {
            return Err(MetaError::Invalid("file path contains another file"));
        }
        let portable: Vec<String> = path.iter().map(|part| part.to_lowercase()).collect();
        if portable_paths
            .range(portable.clone()..)
            .nth(1)
            .is_some_and(|next| next.starts_with(&portable))
        {
            return Err(MetaError::Invalid("case-colliding file directory"));
        }
    }
    Ok(())
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
        assert_eq!(
            meta.info_hash.0,
            [
                0x29, 0xa1, 0xa4, 0x49, 0x20, 0xa1, 0xe2, 0xbe, 0x8c, 0x20, 0xf5, 0x73, 0x09, 0xec,
                0x61, 0x4f, 0x17, 0x6e, 0x81, 0x4f,
            ]
        );
        assert!(meta.trackers.is_empty());
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
        for name in [
            b"CON".as_slice(),
            b"NUL.txt",
            b"COM1.log",
            b"trailing.",
            b"trailing ",
            b"bad?name",
            b"C:\\absolute",
            b"//server/share",
            b"a/b",
            b"a\\b",
            b".",
            b"..",
            b"",
            b"bad\0name",
        ] {
            let mut data = format!("d4:infod6:lengthi1e4:name{}:", name.len()).into_bytes();
            data.extend_from_slice(name);
            data.extend_from_slice(b"12:piece lengthi1e6:pieces20:");
            data.extend([0u8; 20]);
            data.extend_from_slice(b"ee");
            assert!(parse(&data, MetaLimits::default()).is_err());
        }
    }

    #[test]
    fn rejects_duplicate_files_and_file_directory_collisions() {
        let file = |path: &[&str]| TorrentFile {
            path: path.iter().map(|part| (*part).to_owned()).collect(),
            length: 1,
        };
        assert!(validate_file_layout(&[file(&["root", "a"]), file(&["root", "a"])]).is_err());
        assert!(
            validate_file_layout(&[file(&["root", "a"]), file(&["root", "a", "child"])]).is_err()
        );
        assert!(
            validate_file_layout(&[file(&["root", "a", "child"]), file(&["root", "a"])]).is_err()
        );
        assert!(validate_file_layout(&[file(&["root", "A"]), file(&["root", "a"])]).is_err());
    }

    #[test]
    fn retains_bounded_deduplicated_announce_tiers() {
        let mut data = b"d8:announce14:http://a.i2p/a13:announce-listll14:http://a.i2p/a14:http://b.i2p/bee4:infod6:lengthi1e4:name1:x12:piece lengthi1e6:pieces20:".to_vec();
        data.extend([0u8; 20]);
        data.extend_from_slice(b"ee");
        let parsed = parse(&data, MetaLimits::default()).unwrap();
        assert_eq!(parsed.trackers, ["http://a.i2p/a", "http://b.i2p/b"]);
        let limits = MetaLimits {
            announce_count: 1,
            ..MetaLimits::default()
        };
        assert!(matches!(parse(&data, limits), Err(MetaError::Limit)));
    }
}
