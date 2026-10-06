//! Magnet metadata assembly with BEP 9 bounds and exact infohash verification.
use i2pr_tc_core::{
    InfoHashV1,
    extension::{MetadataLimits, UtMetadata, parse_ut_metadata},
    metainfo::{self, MetaLimits},
};
use sha1::{Digest, Sha1};
use std::collections::BTreeSet;
use thiserror::Error;

pub const METADATA_BLOCK_BYTES: usize = 16 * 1024;
pub const MAX_METADATA_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MetadataError {
    #[error("metadata piece was not requested or is invalid")]
    Invalid,
    #[error("metadata exceeds its configured bound")]
    Limit,
    #[error("metadata does not match the magnet infohash")]
    HashMismatch,
}

pub struct MetadataAssembler {
    expected_hash: InfoHashV1,
    max_size: usize,
    total_size: Option<usize>,
    blocks: Vec<Option<Vec<u8>>>,
    in_flight: BTreeSet<u32>,
}

impl MetadataAssembler {
    pub fn new(expected_hash: InfoHashV1, max_size: usize) -> Result<Self, MetadataError> {
        if max_size == 0 || max_size > MAX_METADATA_BYTES {
            return Err(MetadataError::Limit);
        }
        Ok(Self {
            expected_hash,
            max_size,
            total_size: None,
            blocks: Vec::new(),
            in_flight: BTreeSet::new(),
        })
    }

    pub fn set_size(&mut self, size: usize) -> Result<(), MetadataError> {
        if size == 0 || size > self.max_size {
            return Err(MetadataError::Limit);
        }
        if let Some(previous) = self.total_size {
            if previous != size {
                return Err(MetadataError::Invalid);
            }
            return Ok(());
        }
        let count = size.div_ceil(METADATA_BLOCK_BYTES);
        self.blocks.resize_with(count, || None);
        self.total_size = Some(size);
        Ok(())
    }

    pub fn next_requests(&mut self, max_in_flight: usize) -> Vec<u32> {
        let available = max_in_flight.saturating_sub(self.in_flight.len());
        let mut requests = Vec::new();
        for (piece, block) in self.blocks.iter().enumerate() {
            if requests.len() >= available {
                break;
            }
            if block.is_none() && self.in_flight.insert(piece as u32) {
                requests.push(piece as u32);
            }
        }
        requests
    }

    pub fn accept_payload(&mut self, payload: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
        let parsed = parse_ut_metadata(
            payload,
            MetadataLimits {
                total_size: self.max_size as u32,
                block_size: METADATA_BLOCK_BYTES,
                encoded_header: 1024,
            },
        )
        .map_err(|_| MetadataError::Invalid)?;
        match parsed {
            UtMetadata::Reject { piece } => {
                if !self.in_flight.remove(&piece) {
                    return Err(MetadataError::Invalid);
                }
                Ok(None)
            }
            UtMetadata::Request { .. } => Err(MetadataError::Invalid),
            UtMetadata::Data {
                piece,
                total_size,
                block,
            } => {
                if !self.in_flight.remove(&piece) {
                    return Err(MetadataError::Invalid);
                }
                self.set_size(total_size as usize)?;
                let slot = self
                    .blocks
                    .get_mut(piece as usize)
                    .ok_or(MetadataError::Invalid)?;
                if slot.is_some() {
                    return Err(MetadataError::Invalid);
                }
                *slot = Some(block);
                if self.blocks.iter().any(Option::is_none) {
                    return Ok(None);
                }
                let mut info = Vec::with_capacity(self.total_size.ok_or(MetadataError::Invalid)?);
                for block in &self.blocks {
                    info.extend_from_slice(block.as_ref().ok_or(MetadataError::Invalid)?);
                }
                let size = self.total_size.ok_or(MetadataError::Invalid)?;
                if info.len() != size
                    || <[u8; 20]>::from(Sha1::digest(&info)) != self.expected_hash.0
                {
                    self.blocks.fill(None);
                    return Err(MetadataError::HashMismatch);
                }
                let mut metainfo_bytes = Vec::with_capacity(info.len() + 12);
                metainfo_bytes.extend_from_slice(b"d4:info");
                metainfo_bytes.extend_from_slice(&info);
                metainfo_bytes.push(b'e');
                let parsed = metainfo::parse(
                    &metainfo_bytes,
                    MetaLimits {
                        encoded: self.max_size.saturating_add(16),
                        ..Default::default()
                    },
                )
                .map_err(|_| MetadataError::Invalid)?;
                if parsed.info_hash != self.expected_hash {
                    return Err(MetadataError::HashMismatch);
                }
                Ok(Some(metainfo_bytes))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_piece_info() -> Vec<u8> {
        // d6:lengthi1e4:name1:x12:piece lengthi1e6:pieces20:<sha1("x")>e
        let mut info = b"d6:lengthi1e4:name1:x12:piece lengthi1e6:pieces20:".to_vec();
        info.extend_from_slice(&Sha1::digest(b"x"));
        info.push(b'e');
        info
    }

    fn data_message(total_size: usize, piece: usize, data: &[u8]) -> Vec<u8> {
        let mut out =
            format!("d8:msg_typei1e5:piecei{piece}e10:total_sizei{total_size}ee").into_bytes();
        out.extend_from_slice(data);
        out
    }

    #[test]
    fn requests_metadata_in_bounded_windows_and_promotes_only_matching_info() {
        let info = one_piece_info();
        let hash = InfoHashV1(Sha1::digest(&info).into());
        let mut assembler = MetadataAssembler::new(hash, MAX_METADATA_BYTES).unwrap();
        assembler.set_size(info.len()).unwrap();
        assert_eq!(assembler.next_requests(1), vec![0]);
        assert!(assembler.next_requests(1).is_empty());
        let result = assembler
            .accept_payload(&data_message(info.len(), 0, &info))
            .unwrap()
            .unwrap();
        assert_eq!(&result[..7], b"d4:info");
        assert_eq!(
            metainfo::parse(&result, Default::default())
                .unwrap()
                .info_hash,
            hash
        );
    }

    #[test]
    fn rejects_unsolicited_oversize_and_wrong_hash_metadata() {
        let mut assembler = MetadataAssembler::new(InfoHashV1([0; 20]), 1024).unwrap();
        assert_eq!(assembler.set_size(1025), Err(MetadataError::Limit));
        let info = one_piece_info();
        assembler.set_size(info.len()).unwrap();
        assert_eq!(
            assembler.accept_payload(&data_message(info.len(), 0, &info)),
            Err(MetadataError::Invalid)
        );
        assembler.next_requests(1);
        assert_eq!(
            assembler.accept_payload(&data_message(info.len(), 0, &info)),
            Err(MetadataError::HashMismatch)
        );
    }
}
