//! I2P Destination and Destination-hash address semantics.
use crate::{I2pSession, TransportError};
use sha2::{Digest, Sha256};

const MIN_DESTINATION_BYTES: usize = 387;
const MAX_DESTINATION_BYTES: usize = 8192;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Destination {
    bytes: Vec<u8>,
    hash: [u8; 32],
}

impl Destination {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, TransportError> {
        if !(MIN_DESTINATION_BYTES..=MAX_DESTINATION_BYTES).contains(&bytes.len()) {
            return Err(TransportError::Address);
        }
        let hash: [u8; 32] = Sha256::digest(&bytes).into();
        Ok(Self { bytes, hash })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn hash(&self) -> [u8; 32] {
        self.hash
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DestinationHash(pub [u8; 32]);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum I2pAddress {
    Hostname(String),
    Hash(DestinationHash),
    Destination(Destination),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct I2pPeer {
    pub hash: DestinationHash,
    pub destination: Option<Destination>,
}

impl I2pPeer {
    pub fn from_hash(hash: [u8; 32]) -> Self {
        Self {
            hash: DestinationHash(hash),
            destination: None,
        }
    }

    pub fn from_destination(destination: Destination) -> Self {
        Self {
            hash: DestinationHash(destination.hash()),
            destination: Some(destination),
        }
    }
}

pub async fn resolve_peer<S: I2pSession + ?Sized>(
    session: &S,
    peer: &I2pPeer,
) -> Result<Destination, TransportError> {
    if let Some(destination) = &peer.destination {
        if destination.hash() != peer.hash.0 {
            return Err(TransportError::Address);
        }
        return Ok(destination.clone());
    }
    let name = format!("{}.b32.i2p", encode_base32(&peer.hash.0));
    let destination = session.lookup(&name).await?;
    if destination.hash() != peer.hash.0 {
        return Err(TransportError::Address);
    }
    Ok(destination)
}

pub fn validate_i2p_hostname(host: &str) -> Result<(), TransportError> {
    if host.is_empty()
        || host.len() > 1024
        || host
            .chars()
            .any(|character| "@/\\?#:\r\n\0".contains(character))
        || is_ipv4_literal(host)
    {
        return Err(TransportError::Address);
    }
    let lower = host.to_ascii_lowercase();
    if lower.ends_with(".i2p") {
        let name = &host[..host.len() - 4];
        let labels: Vec<_> = name.split('.').collect();
        if labels.is_empty()
            || labels.iter().any(|label| {
                label.is_empty()
                    || label.len() > 63
                    || !label.is_ascii()
                    || label.starts_with('-')
                    || label.ends_with('-')
                    || !label
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
        {
            return Err(TransportError::Address);
        }
        if labels.len() >= 2 && labels[labels.len() - 1].eq_ignore_ascii_case("b32") {
            let encoded = labels[labels.len() - 2];
            if encoded.len() != 52
                || !encoded.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric()
                        && !matches!(byte.to_ascii_lowercase(), b'0' | b'1' | b'8' | b'9')
                })
            {
                return Err(TransportError::Address);
            }
        }
        return Ok(());
    }
    // A raw base64 Destination may appear as a tracker hostname. Its binary
    // parsing is delegated to the router naming adapter, but the token must
    // be base64-shaped and cannot contain authority delimiters.
    let destination = host.strip_suffix(".i2p").unwrap_or(host);
    if destination.len() >= 516
        && destination.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=' | b'-' | b'_')
        })
    {
        return Ok(());
    }
    Err(TransportError::Address)
}

fn is_ipv4_literal(host: &str) -> bool {
    let parts: Vec<_> = host.split('.').collect();
    parts.len() == 4
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

pub fn encode_base32(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut out = String::with_capacity(bytes.len().div_ceil(5) * 8);
    let (mut acc, mut bits) = (0u32, 0u8);
    for byte in bytes {
        acc = (acc << 8) | *byte as u32;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(ALPHABET[((acc >> bits) & 31) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(ALPHABET[((acc << (5 - bits)) & 31) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base32_destination_hash_has_i2p_canonical_length() {
        assert_eq!(encode_base32(&[0; 32]).len(), 52);
        assert!(format!("{}.b32.i2p", encode_base32(&[0; 32])).ends_with(".b32.i2p"));
    }

    #[test]
    fn rejects_ip_and_non_i2p_names() {
        for host in [
            "example.com",
            "1.2.3.4",
            "[::1]",
            "user@tracker.i2p",
            "tracker.i2p/x",
        ] {
            assert!(matches!(
                validate_i2p_hostname(host),
                Err(TransportError::Address)
            ));
        }
        assert!(validate_i2p_hostname("tracker.i2p").is_ok());
        assert!(validate_i2p_hostname("a".repeat(516).as_str()).is_ok());
    }
}
