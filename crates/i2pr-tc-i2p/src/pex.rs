//! I2P PEX payloads and deduplicated peer-source tracking.
use crate::identity::{DestinationHash, I2pPeer};
use i2pr_tc_core::{bencode, wire::I2pPex};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PexError {
    #[error("invalid or over-limit i2p_pex payload")]
    Invalid,
}

pub fn decode_i2p_pex(payload: &[u8], limit: usize) -> Result<I2pPex, PexError> {
    if payload.len() > 64 * 1024 {
        return Err(PexError::Invalid);
    }
    let root = bencode::parse(
        payload,
        bencode::Limits {
            input: 64 * 1024,
            string: 64 * 1024,
            items: 64,
            ..Default::default()
        },
    )
    .map_err(|_| PexError::Invalid)?;
    let added = bytes_field(&root, payload, b"added")?;
    let dropped = bytes_field(&root, payload, b"dropped")?;
    I2pPex::decode(added, dropped, limit).map_err(|_| PexError::Invalid)
}

fn bytes_field<'a>(
    root: &'a bencode::Value,
    input: &'a [u8],
    name: &[u8],
) -> Result<&'a [u8], PexError> {
    match bencode::dict_get(root, input, name) {
        Some(value) => bencode::bytes(value, input).ok_or(PexError::Invalid),
        None => Ok(&[]),
    }
}

pub fn encode_i2p_pex(pex: &I2pPex) -> Vec<u8> {
    let added: Vec<u8> = pex
        .added
        .iter()
        .flat_map(|peer| peer.iter().copied())
        .collect();
    let dropped: Vec<u8> = pex
        .dropped
        .iter()
        .flat_map(|peer| peer.iter().copied())
        .collect();
    let mut output = b"d5:added".to_vec();
    output.extend_from_slice(format!("{}:", added.len()).as_bytes());
    output.extend_from_slice(&added);
    output.extend_from_slice(b"7:dropped");
    output.extend_from_slice(format!("{}:", dropped.len()).as_bytes());
    output.extend_from_slice(&dropped);
    output.push(b'e');
    output
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerSources {
    pub tracker: bool,
    pub pex: bool,
}

#[derive(Default)]
pub struct PeerSourceSet {
    peers: BTreeMap<DestinationHash, PeerSources>,
    failed_until: BTreeMap<DestinationHash, tokio::time::Instant>,
}

impl PeerSourceSet {
    pub fn add_tracker_peers(&mut self, peers: impl IntoIterator<Item = I2pPeer>) {
        for peer in peers {
            self.peers.entry(peer.hash).or_default().tracker = true;
        }
    }

    pub fn apply_pex(
        &mut self,
        pex: &I2pPex,
        local: DestinationHash,
        limit: usize,
    ) -> Result<Vec<I2pPeer>, PexError> {
        if pex.added.len() > limit || pex.dropped.len() > limit {
            return Err(PexError::Invalid);
        }
        for hash in &pex.dropped {
            if let Some(source) = self.peers.get_mut(&DestinationHash(*hash)) {
                source.pex = false;
                if !source.tracker {
                    self.peers.remove(&DestinationHash(*hash));
                }
            }
        }
        let mut additions = Vec::new();
        let mut seen = BTreeSet::new();
        for hash in &pex.added {
            let key = DestinationHash(*hash);
            if key == local || !seen.insert(key) || self.failed_until.contains_key(&key) {
                continue;
            }
            let source = self.peers.entry(key).or_default();
            if !source.tracker && !source.pex {
                source.pex = true;
                additions.push(I2pPeer::from_hash(*hash));
            } else {
                source.pex = true;
            }
        }
        Ok(additions)
    }

    pub fn mark_failed(&mut self, peer: DestinationHash, until: tokio::time::Instant) {
        self.failed_until.insert(peer, until);
    }

    pub fn clear_expired_failures(&mut self, now: tokio::time::Instant) {
        self.failed_until.retain(|_, until| *until > now);
    }

    pub fn sources(&self, peer: DestinationHash) -> Option<PeerSources> {
        self.peers.get(&peer).copied()
    }

    /// Peer hashes suitable for an outgoing I2P PEX message, excluding the
    /// recipient and respecting the protocol's hard per-message bound.
    pub fn advertisable(&self, recipient: DestinationHash, limit: usize) -> Vec<[u8; 32]> {
        self.peers
            .iter()
            .filter(|(hash, sources)| **hash != recipient && (sources.tracker || sources.pex))
            .take(limit)
            .map(|(hash, _)| hash.0)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pex_uses_only_bounded_hashes_and_source_deduplication() {
        let pex = I2pPex {
            added: vec![[1; 32], [2; 32], [1; 32]],
            dropped: vec![[2; 32]],
        };
        let encoded = encode_i2p_pex(&pex);
        assert_eq!(decode_i2p_pex(&encoded, 4).unwrap(), pex);
        assert!(decode_i2p_pex(&[0; 6], 4).is_err());
        let mut sources = PeerSourceSet::default();
        sources.add_tracker_peers([I2pPeer::from_hash([1; 32])]);
        let added = sources
            .apply_pex(&pex, DestinationHash([9; 32]), 4)
            .unwrap();
        assert_eq!(added, vec![I2pPeer::from_hash([2; 32])]);
        assert_eq!(
            sources.sources(DestinationHash([1; 32])),
            Some(PeerSources {
                tracker: true,
                pex: true
            })
        );
        assert_eq!(
            sources.sources(DestinationHash([2; 32])),
            Some(PeerSources {
                tracker: false,
                pex: true
            })
        );
        sources.mark_failed(
            DestinationHash([3; 32]),
            tokio::time::Instant::now() + std::time::Duration::from_secs(60),
        );
        assert!(
            sources
                .apply_pex(
                    &I2pPex {
                        added: vec![[3; 32]],
                        dropped: vec![],
                    },
                    DestinationHash([9; 32]),
                    4,
                )
                .unwrap()
                .is_empty()
        );
    }
}
