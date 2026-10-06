//! Bounded BitTorrent v1 peer-wire framing and I2P-specific identity payloads.
use thiserror::Error;

pub const HANDSHAKE_LEN: usize = 68;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    Port(u16),
    Extension {
        id: u8,
        payload: Vec<u8>,
    },
    Unknown {
        id: u8,
        payload: Vec<u8>,
    },
}
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WireError {
    #[error("frame exceeds limit")]
    Limit,
    #[error("invalid peer handshake or frame")]
    Invalid,
    #[error("truncated frame")]
    Truncated,
    #[error("frame decoder has not been configured with a positive bound")]
    InvalidLimit,
}

/// Incremental stream decoder. It accepts fragmented or coalesced peer-wire
/// frames while retaining at most one bounded frame between calls.
pub struct FrameDecoder {
    buffer: Vec<u8>,
    expected: Option<usize>,
    max_frame: usize,
}

impl FrameDecoder {
    pub fn new(max_frame: usize) -> Result<Self, WireError> {
        if max_frame == 0 {
            return Err(WireError::InvalidLimit);
        }
        Ok(Self {
            buffer: Vec::with_capacity(max_frame.min(16 * 1024) + 4),
            expected: None,
            max_frame,
        })
    }

    pub fn feed(&mut self, mut input: &[u8]) -> Result<Vec<Message>, WireError> {
        let mut messages = Vec::new();
        while !input.is_empty() {
            let target = self.expected.unwrap_or(4);
            let take = (target - self.buffer.len()).min(input.len());
            self.buffer.extend_from_slice(&input[..take]);
            input = &input[take..];

            if self.expected.is_none() && self.buffer.len() == 4 {
                let length = u32::from_be_bytes(self.buffer[..4].try_into().unwrap()) as usize;
                if length > self.max_frame {
                    self.buffer.clear();
                    return Err(WireError::Limit);
                }
                let total = length.checked_add(4).ok_or(WireError::Limit)?;
                self.expected = Some(total);
                if length == 0 {
                    messages.push(Message::KeepAlive);
                    self.buffer.clear();
                    self.expected = None;
                }
            } else if self.expected == Some(self.buffer.len()) {
                messages.push(parse_frame(&self.buffer, self.max_frame)?);
                self.buffer.clear();
                self.expected = None;
            }
        }
        Ok(messages)
    }

    pub fn has_partial_frame(&self) -> bool {
        !self.buffer.is_empty()
    }
}
pub fn parse_handshake(b: &[u8]) -> Result<Handshake, WireError> {
    if b.len() != HANDSHAKE_LEN || b[0] != 19 || &b[1..20] != b"BitTorrent protocol" {
        return Err(WireError::Invalid);
    }
    Ok(Handshake {
        reserved: b[20..28].try_into().unwrap(),
        info_hash: b[28..48].try_into().unwrap(),
        peer_id: b[48..68].try_into().unwrap(),
    })
}
pub fn encode_handshake(h: &Handshake) -> [u8; HANDSHAKE_LEN] {
    let mut b = [0; HANDSHAKE_LEN];
    b[0] = 19;
    b[1..20].copy_from_slice(b"BitTorrent protocol");
    b[20..28].copy_from_slice(&h.reserved);
    b[28..48].copy_from_slice(&h.info_hash);
    b[48..68].copy_from_slice(&h.peer_id);
    b
}
pub fn parse_frame(frame: &[u8], max: usize) -> Result<Message, WireError> {
    if frame.len() < 4 {
        return Err(WireError::Truncated);
    }
    let n = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
    if n > max {
        return Err(WireError::Limit);
    }
    if frame.len() != n.checked_add(4).ok_or(WireError::Limit)? {
        return Err(WireError::Truncated);
    }
    if n == 0 {
        return Ok(Message::KeepAlive);
    }
    let id = frame[4];
    let p = &frame[5..];
    let u32at = |i: usize| -> Result<u32, WireError> {
        Ok(u32::from_be_bytes(
            p.get(i..i + 4)
                .ok_or(WireError::Invalid)?
                .try_into()
                .unwrap(),
        ))
    };
    Ok(match id {
        0 if p.is_empty() => Message::Choke,
        1 if p.is_empty() => Message::Unchoke,
        2 if p.is_empty() => Message::Interested,
        3 if p.is_empty() => Message::NotInterested,
        4 if p.len() == 4 => Message::Have(u32at(0)?),
        5 => Message::Bitfield(p.to_vec()),
        6 if p.len() == 12 => Message::Request {
            index: u32at(0)?,
            begin: u32at(4)?,
            length: u32at(8)?,
        },
        7 if p.len() >= 8 => Message::Piece {
            index: u32at(0)?,
            begin: u32at(4)?,
            block: p[8..].to_vec(),
        },
        8 if p.len() == 12 => Message::Cancel {
            index: u32at(0)?,
            begin: u32at(4)?,
            length: u32at(8)?,
        },
        9 if p.len() == 2 => Message::Port(u16::from_be_bytes(p.try_into().unwrap())),
        20 if !p.is_empty() => Message::Extension {
            id: p[0],
            payload: p[1..].to_vec(),
        },
        x => Message::Unknown {
            id: x,
            payload: p.to_vec(),
        },
    })
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct I2pPex {
    pub added: Vec<[u8; 32]>,
    pub dropped: Vec<[u8; 32]>,
}
impl I2pPex {
    pub fn decode(added: &[u8], dropped: &[u8], limit: usize) -> Result<Self, WireError> {
        if added.len() % 32 != 0
            || dropped.len() % 32 != 0
            || added.len() / 32 > limit
            || dropped.len() / 32 > limit
        {
            return Err(WireError::Limit);
        }
        Ok(Self {
            added: added
                .chunks_exact(32)
                .map(|x| x.try_into().unwrap())
                .collect(),
            dropped: dropped
                .chunks_exact(32)
                .map(|x| x.try_into().unwrap())
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn handshake_round_trip_and_frame_bounds() {
        let h = Handshake {
            reserved: [0; 8],
            info_hash: [1; 20],
            peer_id: [2; 20],
        };
        assert_eq!(parse_handshake(&encode_handshake(&h)).unwrap(), h);
        assert_eq!(
            parse_frame(&[0, 0, 0, 0], 1024).unwrap(),
            Message::KeepAlive
        );
        assert_eq!(parse_frame(&[0, 0, 0, 1], 1024), Err(WireError::Truncated));
        assert_eq!(parse_frame(&[0, 0, 0, 5, 5], 4), Err(WireError::Limit));
    }

    #[test]
    fn incremental_decoder_handles_fragmented_and_coalesced_frames() {
        let mut decoder = FrameDecoder::new(64).unwrap();
        assert!(decoder.feed(&[0, 0]).unwrap().is_empty());
        assert!(decoder.has_partial_frame());
        assert!(decoder.feed(&[0, 1]).unwrap().is_empty());
        assert!(decoder.has_partial_frame());
        assert_eq!(
            decoder.feed(&[0, 0, 0, 0, 1, 1, 0, 0, 0, 0]).unwrap(),
            vec![Message::Choke, Message::Unchoke, Message::KeepAlive]
        );
        assert!(!decoder.has_partial_frame());
    }

    #[test]
    fn incremental_decoder_rejects_declared_oversize_before_payload() {
        let mut decoder = FrameDecoder::new(8).unwrap();
        assert_eq!(decoder.feed(&[0, 0, 0, 9]), Err(WireError::Limit));
        assert!(!decoder.has_partial_frame());
    }
    #[test]
    fn pex_uses_only_hash_sized_records() {
        assert!(I2pPex::decode(&[0; 6], &[], 10).is_err());
        let p = I2pPex::decode(&[7; 32], &[], 1).unwrap();
        assert_eq!(p.added[0], [7; 32]);
    }
}
