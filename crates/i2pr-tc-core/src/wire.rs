//! Bounded BitTorrent v1 peer-wire framing and I2P-specific identity payloads.
use std::collections::BTreeSet;
use thiserror::Error;

pub const HANDSHAKE_LEN: usize = 68;
const MAX_MESSAGES_PER_FEED: usize = 4096;
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
            if messages.len() >= MAX_MESSAGES_PER_FEED {
                return Err(WireError::Limit);
            }
            let target = self.expected.unwrap_or(4);
            let take = (target - self.buffer.len()).min(input.len());
            self.buffer.extend_from_slice(&input[..take]);
            input = &input[take..];

            if self.expected.is_none() && self.buffer.len() == 4 {
                let length = u32::from_be_bytes(self.buffer[..4].try_into().unwrap()) as usize;
                if length > self.max_frame {
                    self.reset();
                    return Err(WireError::Limit);
                }
                let Some(total) = length.checked_add(4) else {
                    self.reset();
                    return Err(WireError::Limit);
                };
                self.expected = Some(total);
                if length == 0 {
                    messages.push(Message::KeepAlive);
                    self.buffer.clear();
                    self.expected = None;
                }
            } else if self.expected == Some(self.buffer.len()) {
                match parse_frame(&self.buffer, self.max_frame) {
                    Ok(message) => {
                        messages.push(message);
                        self.reset();
                    }
                    Err(error) => {
                        self.reset();
                        return Err(error);
                    }
                }
            }
        }
        Ok(messages)
    }

    pub fn has_partial_frame(&self) -> bool {
        !self.buffer.is_empty()
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.expected = None;
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

pub fn encode_frame(message: &Message, max_frame: usize) -> Result<Vec<u8>, WireError> {
    let payload_len = match message {
        Message::KeepAlive => 0,
        Message::Choke | Message::Unchoke | Message::Interested | Message::NotInterested => 1,
        Message::Have(_) => 5,
        Message::Bitfield(bits) => 1usize.checked_add(bits.len()).ok_or(WireError::Limit)?,
        Message::Request { .. } | Message::Cancel { .. } => 13,
        Message::Piece { block, .. } => 9usize.checked_add(block.len()).ok_or(WireError::Limit)?,
        Message::Port(_) => 3,
        Message::Extension { payload, .. } => {
            2usize.checked_add(payload.len()).ok_or(WireError::Limit)?
        }
        Message::Unknown { payload, .. } => {
            1usize.checked_add(payload.len()).ok_or(WireError::Limit)?
        }
    };
    if matches!(message, Message::Unknown { id: 0..=9 | 20, .. }) {
        return Err(WireError::Invalid);
    }
    if payload_len > max_frame || payload_len > u32::MAX as usize {
        return Err(WireError::Limit);
    }
    let frame_len = payload_len.checked_add(4).ok_or(WireError::Limit)?;
    let mut frame = Vec::with_capacity(frame_len);
    frame.extend_from_slice(&(payload_len as u32).to_be_bytes());
    let put_u32 = |out: &mut Vec<u8>, n: u32| out.extend_from_slice(&n.to_be_bytes());
    match message {
        Message::KeepAlive => {}
        Message::Choke => frame.push(0),
        Message::Unchoke => frame.push(1),
        Message::Interested => frame.push(2),
        Message::NotInterested => frame.push(3),
        Message::Have(index) => {
            frame.push(4);
            put_u32(&mut frame, *index);
        }
        Message::Bitfield(bits) => {
            frame.push(5);
            frame.extend_from_slice(bits);
        }
        Message::Request {
            index,
            begin,
            length,
        } => {
            frame.push(6);
            put_u32(&mut frame, *index);
            put_u32(&mut frame, *begin);
            put_u32(&mut frame, *length);
        }
        Message::Piece {
            index,
            begin,
            block,
        } => {
            frame.push(7);
            put_u32(&mut frame, *index);
            put_u32(&mut frame, *begin);
            frame.extend_from_slice(block);
        }
        Message::Cancel {
            index,
            begin,
            length,
        } => {
            frame.push(8);
            put_u32(&mut frame, *index);
            put_u32(&mut frame, *begin);
            put_u32(&mut frame, *length);
        }
        Message::Port(port) => {
            frame.push(9);
            frame.extend_from_slice(&port.to_be_bytes());
        }
        Message::Extension { id, payload } => {
            frame.push(20);
            frame.push(*id);
            frame.extend_from_slice(payload);
        }
        Message::Unknown { id, payload } => {
            frame.push(*id);
            frame.extend_from_slice(payload);
        }
    }
    Ok(frame)
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
        0..=9 | 20 => return Err(WireError::Invalid),
        x => Message::Unknown {
            id: x,
            payload: p.to_vec(),
        },
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerEvent {
    KeepAlive,
    Choked,
    Unchoked,
    PeerInterested(bool),
    Have(u32),
    Bitfield(Vec<bool>),
    UploadRequest {
        index: u32,
        begin: u32,
        length: u32,
    },
    UploadCancelled {
        index: u32,
        begin: u32,
        length: u32,
    },
    DownloadPiece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Port(u16),
    Extension {
        id: u8,
        payload: Vec<u8>,
    },
    IgnoredUnknown {
        id: u8,
    },
}

/// Stateful validation for one peer-wire connection. Peer identity and transport remain outside
/// this codec; the caller binds the validated handshake to its I2P Destination.
pub struct PeerWireSession {
    info_hash: [u8; 20],
    piece_length: u32,
    total_length: u64,
    piece_count: usize,
    max_block: u32,
    max_pending: usize,
    handshake_complete: bool,
    remote_choking: bool,
    remote_interested: bool,
    remote_pieces: Vec<bool>,
    bitfield_received: bool,
    have_received: bool,
    pending_downloads: BTreeSet<(u32, u32, u32)>,
}

impl PeerWireSession {
    pub fn new(
        info_hash: [u8; 20],
        piece_length: u32,
        total_length: u64,
        max_block: u32,
        max_pending: usize,
    ) -> Result<Self, WireError> {
        if piece_length == 0 || max_block == 0 || max_pending == 0 {
            return Err(WireError::InvalidLimit);
        }
        let piece_count = if total_length == 0 {
            0
        } else {
            let count = 1 + (total_length - 1) / piece_length as u64;
            if count > 4_000_000 {
                return Err(WireError::Limit);
            }
            count as usize
        };
        Ok(Self {
            info_hash,
            piece_length,
            total_length,
            piece_count,
            max_block,
            max_pending,
            handshake_complete: false,
            remote_choking: true,
            remote_interested: false,
            remote_pieces: vec![false; piece_count],
            bitfield_received: false,
            have_received: false,
            pending_downloads: BTreeSet::new(),
        })
    }

    pub fn accept_handshake(&mut self, bytes: &[u8]) -> Result<Handshake, WireError> {
        if self.handshake_complete {
            return Err(WireError::Invalid);
        }
        let handshake = parse_handshake(bytes)?;
        if handshake.info_hash != self.info_hash {
            return Err(WireError::Invalid);
        }
        self.handshake_complete = true;
        Ok(handshake)
    }

    pub fn request_block(
        &mut self,
        index: u32,
        begin: u32,
        length: u32,
    ) -> Result<Message, WireError> {
        if !self.handshake_complete
            || self.remote_choking
            || !self.valid_block(index, begin, length)
        {
            return Err(WireError::Invalid);
        }
        if self.pending_downloads.len() >= self.max_pending
            || !self.pending_downloads.insert((index, begin, length))
        {
            return Err(WireError::Limit);
        }
        Ok(Message::Request {
            index,
            begin,
            length,
        })
    }

    pub fn cancel_block(&mut self, index: u32, begin: u32, length: u32) -> Option<Message> {
        self.pending_downloads
            .remove(&(index, begin, length))
            .then_some(Message::Cancel {
                index,
                begin,
                length,
            })
    }

    pub fn clear_pending_downloads(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.pending_downloads)
            .into_iter()
            .map(|(index, begin, length)| Message::Cancel {
                index,
                begin,
                length,
            })
            .collect()
    }

    pub fn on_message(&mut self, message: Message) -> Result<PeerEvent, WireError> {
        if !self.handshake_complete {
            return Err(WireError::Invalid);
        }
        match message {
            Message::KeepAlive => Ok(PeerEvent::KeepAlive),
            Message::Choke => {
                self.remote_choking = true;
                Ok(PeerEvent::Choked)
            }
            Message::Unchoke => {
                self.remote_choking = false;
                Ok(PeerEvent::Unchoked)
            }
            Message::Interested => {
                self.remote_interested = true;
                Ok(PeerEvent::PeerInterested(true))
            }
            Message::NotInterested => {
                self.remote_interested = false;
                Ok(PeerEvent::PeerInterested(false))
            }
            Message::Have(index) => {
                let slot = self
                    .remote_pieces
                    .get_mut(index as usize)
                    .ok_or(WireError::Invalid)?;
                *slot = true;
                self.have_received = true;
                Ok(PeerEvent::Have(index))
            }
            Message::Bitfield(bytes) => {
                let expected = self.piece_count.div_ceil(8);
                if self.bitfield_received || self.have_received || bytes.len() != expected {
                    return Err(WireError::Invalid);
                }
                if self.piece_count % 8 != 0 && !bytes.is_empty() {
                    let padding = 8 - self.piece_count % 8;
                    if bytes[bytes.len() - 1] & ((1u8 << padding) - 1) != 0 {
                        return Err(WireError::Invalid);
                    }
                }
                for (index, has) in self.remote_pieces.iter_mut().enumerate() {
                    *has = bytes[index / 8] & (0x80 >> (index % 8)) != 0;
                }
                self.bitfield_received = true;
                Ok(PeerEvent::Bitfield(self.remote_pieces.clone()))
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                if !self.valid_block(index, begin, length) {
                    return Err(WireError::Invalid);
                }
                Ok(PeerEvent::UploadRequest {
                    index,
                    begin,
                    length,
                })
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                if !self.valid_block(index, begin, length) {
                    return Err(WireError::Invalid);
                }
                Ok(PeerEvent::UploadCancelled {
                    index,
                    begin,
                    length,
                })
            }
            Message::Piece {
                index,
                begin,
                block,
            } => {
                let length = u32::try_from(block.len()).map_err(|_| WireError::Limit)?;
                if !self.valid_block(index, begin, length)
                    || !self.pending_downloads.remove(&(index, begin, length))
                {
                    return Err(WireError::Invalid);
                }
                Ok(PeerEvent::DownloadPiece {
                    index,
                    begin,
                    block,
                })
            }
            Message::Port(port) => Ok(PeerEvent::Port(port)),
            Message::Extension { id, payload } => Ok(PeerEvent::Extension { id, payload }),
            Message::Unknown { id, .. } => Ok(PeerEvent::IgnoredUnknown { id }),
        }
    }

    pub fn remote_pieces(&self) -> &[bool] {
        &self.remote_pieces
    }

    pub fn remote_interested(&self) -> bool {
        self.remote_interested
    }

    fn valid_block(&self, index: u32, begin: u32, length: u32) -> bool {
        if length == 0 || length > self.max_block || index as usize >= self.piece_count {
            return false;
        }
        let offset = index as u64 * self.piece_length as u64;
        let piece_size = (self.total_length - offset).min(self.piece_length as u64) as u32;
        begin
            .checked_add(length)
            .is_some_and(|end| end <= piece_size)
    }
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
    fn malformed_frame_resets_the_incremental_decoder() {
        let mut decoder = FrameDecoder::new(64).unwrap();
        assert_eq!(decoder.feed(&[0, 0, 0, 2, 0, 1]), Err(WireError::Invalid));
        assert!(!decoder.has_partial_frame());
        assert_eq!(
            decoder.feed(&[0, 0, 0, 1, 0]).unwrap(),
            vec![Message::Choke]
        );
    }
    #[test]
    fn pex_uses_only_hash_sized_records() {
        assert!(I2pPex::decode(&[0; 6], &[], 10).is_err());
        let p = I2pPex::decode(&[7; 32], &[], 1).unwrap();
        assert_eq!(p.added[0], [7; 32]);
    }

    #[test]
    fn malformed_known_message_ids_fail_closed() {
        assert_eq!(
            parse_frame(&[0, 0, 0, 2, 0, 1], 32),
            Err(WireError::Invalid)
        );
        assert_eq!(parse_frame(&[0, 0, 0, 1, 20], 32), Err(WireError::Invalid));
        assert_eq!(
            parse_frame(&[0, 0, 0, 1, 42], 32),
            Ok(Message::Unknown {
                id: 42,
                payload: vec![]
            })
        );
    }

    #[test]
    fn every_supported_message_encodes_and_decodes_with_a_frame_bound() {
        let messages = vec![
            Message::KeepAlive,
            Message::Choke,
            Message::Unchoke,
            Message::Interested,
            Message::NotInterested,
            Message::Have(7),
            Message::Bitfield(vec![0x80]),
            Message::Request {
                index: 1,
                begin: 2,
                length: 3,
            },
            Message::Piece {
                index: 1,
                begin: 2,
                block: b"abc".to_vec(),
            },
            Message::Cancel {
                index: 1,
                begin: 2,
                length: 3,
            },
            Message::Port(1234),
            Message::Extension {
                id: 3,
                payload: b"x".to_vec(),
            },
            Message::Unknown {
                id: 42,
                payload: b"y".to_vec(),
            },
        ];
        for message in messages {
            let encoded = encode_frame(&message, 1024).unwrap();
            assert_eq!(parse_frame(&encoded, 1024).unwrap(), message);
            if message != Message::KeepAlive {
                assert_eq!(encode_frame(&message, 0), Err(WireError::Limit));
            }
        }
    }

    #[test]
    fn peer_session_validates_handshake_requests_and_piece_responses() {
        let mut session = PeerWireSession::new([1; 20], 4, 6, 4, 2).unwrap();
        assert_eq!(
            session.on_message(Message::Unchoke),
            Err(WireError::Invalid)
        );
        let bad = Handshake {
            reserved: [0; 8],
            info_hash: [2; 20],
            peer_id: [3; 20],
        };
        assert_eq!(
            session.accept_handshake(&encode_handshake(&bad)),
            Err(WireError::Invalid)
        );
        let good = Handshake {
            info_hash: [1; 20],
            ..bad
        };
        session.accept_handshake(&encode_handshake(&good)).unwrap();
        assert_eq!(session.request_block(0, 0, 1), Err(WireError::Invalid));
        assert_eq!(
            session.on_message(Message::Unchoke),
            Ok(PeerEvent::Unchoked)
        );
        let request = session.request_block(1, 0, 2).unwrap();
        assert_eq!(
            request,
            Message::Request {
                index: 1,
                begin: 0,
                length: 2
            }
        );
        assert_eq!(
            session.on_message(Message::Piece {
                index: 1,
                begin: 0,
                block: b"ab".to_vec()
            }),
            Ok(PeerEvent::DownloadPiece {
                index: 1,
                begin: 0,
                block: b"ab".to_vec()
            })
        );
        assert_eq!(
            session.on_message(Message::Piece {
                index: 1,
                begin: 0,
                block: b"ab".to_vec()
            }),
            Err(WireError::Invalid)
        );
        let cancellable = session.request_block(0, 0, 4).unwrap();
        assert_eq!(
            session.cancel_block(0, 0, 4),
            Some(Message::Cancel {
                index: 0,
                begin: 0,
                length: 4
            })
        );
        assert_eq!(session.cancel_block(0, 0, 4), None);
        assert!(
            cancellable
                == Message::Request {
                    index: 0,
                    begin: 0,
                    length: 4
                }
        );
        assert!(session
            .on_message(Message::Request {
                index: 1,
                begin: 4,
                length: 1
            })
            .is_err());
    }

    #[test]
    fn peer_session_bounds_bitfields_and_piece_requests() {
        let mut session = PeerWireSession::new([4; 20], 8, 9, 4, 1).unwrap();
        let handshake = Handshake {
            reserved: [0; 8],
            info_hash: [4; 20],
            peer_id: [5; 20],
        };
        session
            .accept_handshake(&encode_handshake(&handshake))
            .unwrap();
        assert_eq!(
            session.on_message(Message::Bitfield(vec![0x81])),
            Err(WireError::Invalid)
        );
        assert_eq!(
            session.on_message(Message::Bitfield(vec![0x80])),
            Ok(PeerEvent::Bitfield(vec![true, false]))
        );
        assert_eq!(
            session.on_message(Message::Bitfield(vec![0])),
            Err(WireError::Invalid)
        );
        session.on_message(Message::Unchoke).unwrap();
        assert!(session.request_block(1, 0, 1).is_ok());
        assert_eq!(session.request_block(1, 0, 1), Err(WireError::Limit));
        assert_eq!(session.request_block(1, 0, 2), Err(WireError::Invalid));
    }
}
