//! Strict bounded BitTorrent v1 magnet URI parsing.
use crate::InfoHashV1;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Magnet {
    pub info_hash: InfoHashV1,
    pub display_name: Option<String>,
    pub trackers: Vec<String>,
}
#[derive(Debug, Error, PartialEq, Eq)]
pub enum MagnetError {
    #[error("invalid magnet URI")]
    Invalid,
    #[error("unsupported exact-topic hash")]
    UnsupportedHash,
    #[error("magnet URI exceeds configured bounds")]
    Limit,
}
#[derive(Clone, Copy, Debug)]
pub struct MagnetLimits {
    pub uri_bytes: usize,
    pub name_bytes: usize,
    pub trackers: usize,
    pub tracker_bytes: usize,
}
impl Default for MagnetLimits {
    fn default() -> Self {
        Self {
            uri_bytes: 8192,
            name_bytes: 1024,
            trackers: 16,
            tracker_bytes: 2048,
        }
    }
}

pub fn parse(uri: &str, limits: MagnetLimits) -> Result<Magnet, MagnetError> {
    if uri.len() > limits.uri_bytes {
        return Err(MagnetError::Limit);
    }
    let query = uri.strip_prefix("magnet:?").ok_or(MagnetError::Invalid)?;
    let mut hash = None;
    let mut name = None;
    let mut trackers = Vec::new();
    for pair in query.split('&') {
        let Some((k, v)) = pair.split_once('=') else {
            return Err(MagnetError::Invalid);
        };
        let key = decode(k)?;
        let value = decode(v)?;
        match key.as_str() {
            "xt" => {
                if let Some(h) = value.strip_prefix("urn:btih:") {
                    if hash.is_some() {
                        return Err(MagnetError::Invalid);
                    }
                    hash = Some(parse_hash(h)?)
                } else if value.starts_with("urn:") {
                    return Err(MagnetError::UnsupportedHash);
                }
            }
            "dn" => {
                if value.len() > limits.name_bytes {
                    return Err(MagnetError::Limit);
                }
                if name.is_none() {
                    name = Some(value)
                }
            }
            "tr" => {
                if value.len() > limits.tracker_bytes || trackers.len() >= limits.trackers {
                    return Err(MagnetError::Limit);
                }
                trackers.push(value)
            }
            _ => {}
        }
    }
    Ok(Magnet {
        info_hash: hash.ok_or(MagnetError::Invalid)?,
        display_name: name,
        trackers,
    })
}
fn decode(s: &str) -> Result<String, MagnetError> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                if i + 2 >= b.len() {
                    return Err(MagnetError::Invalid);
                }
                let hi = hex(b[i + 1]).ok_or(MagnetError::Invalid)?;
                let lo = hex(b[i + 2]).ok_or(MagnetError::Invalid)?;
                out.push((hi << 4) | lo);
                i += 3
            }
            b'+' => {
                out.push(b' ');
                i += 1
            }
            c => {
                out.push(c);
                i += 1
            }
        }
    }
    String::from_utf8(out).map_err(|_| MagnetError::Invalid)
}
fn hex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}
fn parse_hash(s: &str) -> Result<InfoHashV1, MagnetError> {
    if s.len() == 40 {
        return decode_hex_hash(s);
    }
    if s.len() == 32 {
        return decode_base32_hash(s);
    }
    Err(MagnetError::Invalid)
}
fn decode_hex_hash(s: &str) -> Result<InfoHashV1, MagnetError> {
    let mut out = [0; 20];
    for (i, p) in s.as_bytes().chunks_exact(2).enumerate() {
        out[i] =
            (hex(p[0]).ok_or(MagnetError::Invalid)? << 4) | hex(p[1]).ok_or(MagnetError::Invalid)?
    }
    Ok(InfoHashV1(out))
}
fn decode_base32_hash(s: &str) -> Result<InfoHashV1, MagnetError> {
    let mut out = [0u8; 20];
    let (mut acc, mut bits, mut j) = (0u32, 0u8, 0usize);
    for c in s.bytes() {
        let v = match c.to_ascii_uppercase() {
            b'A'..=b'Z' => c.to_ascii_uppercase() - b'A',
            b'2'..=b'7' => c - b'2' + 26,
            _ => return Err(MagnetError::Invalid),
        };
        acc = (acc << 5) | v as u32;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out[j] = (acc >> bits) as u8;
            j += 1;
        }
    }
    if j != 20 || bits != 0 {
        return Err(MagnetError::Invalid);
    }
    Ok(InfoHashV1(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_v1_hash_and_bounded_trackers() {
        let m=parse("magnet:?xt=urn%3Abtih%3A0000000000000000000000000000000000000000&dn=hello+world&tr=http%3A%2F%2Ftracker.i2p%2Fa",MagnetLimits::default()).unwrap();
        assert_eq!(m.info_hash, InfoHashV1([0; 20]));
        assert_eq!(m.display_name.as_deref(), Some("hello world"));
        assert_eq!(m.trackers.len(), 1);
        assert_eq!(
            parse("magnet:?xt=urn:btmh:xyz", MagnetLimits::default()),
            Err(MagnetError::UnsupportedHash)
        );
    }
}
