//! Deterministic bounded piece/block ownership primitives.
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PieceStatus {
    Missing,
    InFlight,
    Verified,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockRequest {
    pub peer: [u8; 32],
    pub piece: u32,
    pub begin: u32,
    pub length: u32,
}
#[derive(Clone, Debug)]
pub struct PieceMap {
    states: Vec<PieceStatus>,
    availability: Vec<u16>,
    peer_pieces: BTreeMap<[u8; 32], Vec<bool>>,
    inflight: BTreeSet<(u32, u32, [u8; 32])>,
    max_inflight: usize,
    max_per_peer: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScheduleError {
    Bounds,
    InFlightLimit,
    InvalidBlock,
}
impl PieceMap {
    pub fn new(
        pieces: usize,
        max_pieces: usize,
        max_inflight: usize,
        max_per_peer: usize,
    ) -> Result<Self, ScheduleError> {
        if pieces > max_pieces {
            return Err(ScheduleError::Bounds);
        }
        Ok(Self {
            states: vec![PieceStatus::Missing; pieces],
            availability: vec![0; pieces],
            peer_pieces: BTreeMap::new(),
            inflight: BTreeSet::new(),
            max_inflight,
            max_per_peer,
        })
    }
    pub fn len(&self) -> usize {
        self.states.len()
    }
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }
    pub fn status(&self, piece: u32) -> Option<PieceStatus> {
        self.states.get(piece as usize).copied()
    }
    pub fn mark_verified(&mut self, piece: u32) -> Result<(), ScheduleError> {
        let s = self
            .states
            .get_mut(piece as usize)
            .ok_or(ScheduleError::Bounds)?;
        *s = PieceStatus::Verified;
        self.inflight.retain(|(p, _, _)| *p != piece);
        Ok(())
    }
    pub fn reset_piece(&mut self, piece: u32) -> Result<(), ScheduleError> {
        let s = self
            .states
            .get_mut(piece as usize)
            .ok_or(ScheduleError::Bounds)?;
        *s = PieceStatus::Missing;
        self.inflight.retain(|(p, _, _)| *p != piece);
        Ok(())
    }
    /// Replace one peer's advertised piece set and update availability counts.
    pub fn set_availability(
        &mut self,
        peer: [u8; 32],
        peer_pieces: &[bool],
    ) -> Result<(), ScheduleError> {
        if peer_pieces.len() != self.states.len() {
            return Err(ScheduleError::Bounds);
        }
        if let Some(previous) = self.peer_pieces.insert(peer, peer_pieces.to_vec()) {
            for (i, had) in previous.iter().enumerate() {
                if *had {
                    self.availability[i] = self.availability[i].saturating_sub(1);
                }
            }
        }
        for (i, has) in peer_pieces.iter().enumerate() {
            if *has {
                self.availability[i] = self.availability[i].saturating_add(1);
            }
        }
        Ok(())
    }
    pub fn disconnect_peer(&mut self, peer: [u8; 32]) {
        if let Some(previous) = self.peer_pieces.remove(&peer) {
            for (i, had) in previous.iter().enumerate() {
                if *had {
                    self.availability[i] = self.availability[i].saturating_sub(1);
                }
            }
        }
        self.cancel_peer_requests(peer);
    }
    pub fn cancel_peer_requests(&mut self, peer: [u8; 32]) {
        self.inflight.retain(|(_, _, p)| *p != peer);
        for (i, s) in self.states.iter_mut().enumerate() {
            if *s == PieceStatus::InFlight && !self.inflight.iter().any(|(p, _, _)| *p == i as u32)
            {
                *s = PieceStatus::Missing
            }
        }
    }
    pub fn choose_piece(&self, peer: [u8; 32]) -> Option<u32> {
        let peer_count = self.inflight.iter().filter(|(_, _, p)| *p == peer).count();
        if peer_count >= self.max_per_peer || self.inflight.len() >= self.max_inflight {
            return None;
        }
        self.states
            .iter()
            .enumerate()
            .filter(|(i, s)| **s == PieceStatus::Missing && self.availability[*i] > 0)
            .min_by_key(|(i, _)| (self.availability[*i], *i))
            .map(|(i, _)| i as u32)
    }
    pub fn request_block(
        &mut self,
        peer: [u8; 32],
        piece: u32,
        begin: u32,
        length: u32,
        piece_size: u32,
        max_block: u32,
    ) -> Result<BlockRequest, ScheduleError> {
        if length == 0
            || length > max_block
            || begin.checked_add(length).is_none_or(|e| e > piece_size)
        {
            return Err(ScheduleError::InvalidBlock);
        }
        if self.inflight.len() >= self.max_inflight
            || self.inflight.iter().filter(|(_, _, p)| *p == peer).count() >= self.max_per_peer
        {
            return Err(ScheduleError::InFlightLimit);
        }
        let s = self
            .states
            .get_mut(piece as usize)
            .ok_or(ScheduleError::Bounds)?;
        if *s == PieceStatus::Verified {
            return Err(ScheduleError::InvalidBlock);
        }
        *s = PieceStatus::InFlight;
        if !self.inflight.insert((piece, begin, peer)) {
            return Err(ScheduleError::InvalidBlock);
        }
        Ok(BlockRequest {
            peer,
            piece,
            begin,
            length,
        })
    }
    pub fn complete_block(&mut self, request: &BlockRequest) {
        self.inflight
            .remove(&(request.piece, request.begin, request.peer));
        if self.states.get(request.piece as usize) == Some(&PieceStatus::InFlight)
            && !self
                .inflight
                .iter()
                .any(|(piece, _, _)| *piece == request.piece)
        {
            self.states[request.piece as usize] = PieceStatus::Missing;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rarest_first_and_disconnect_release() {
        let peer = [4; 32];
        let mut p = PieceMap::new(3, 10, 4, 2).unwrap();
        p.set_availability(peer, &[true, true, true]).unwrap();
        p.set_availability([5; 32], &[false, true, true]).unwrap();
        assert_eq!(p.choose_piece(peer), Some(0));
        let r = p.request_block(peer, 0, 0, 4, 16, 8).unwrap();
        assert_eq!(p.choose_piece(peer), Some(1));
        p.disconnect_peer(peer);
        assert_eq!(p.status(0), Some(PieceStatus::Missing));
        p.complete_block(&r);
        assert_eq!(p.status(0), Some(PieceStatus::Missing));
    }

    #[test]
    fn peer_availability_replaces_and_withdraws_counts() {
        let peer = [1; 32];
        let second = [2; 32];
        let mut pieces = PieceMap::new(2, 4, 8, 4).unwrap();
        pieces.set_availability(peer, &[true, false]).unwrap();
        pieces.set_availability(second, &[false, true]).unwrap();
        assert_eq!(pieces.choose_piece([9; 32]), Some(0));

        pieces.set_availability(peer, &[false, true]).unwrap();
        assert_eq!(pieces.choose_piece([9; 32]), Some(1));

        pieces.disconnect_peer(second);
        assert_eq!(pieces.choose_piece([9; 32]), Some(1));
        pieces.disconnect_peer(peer);
        assert_eq!(pieces.choose_piece([9; 32]), None);
    }

    #[test]
    fn independent_blocks_keep_piece_in_flight_until_all_finish() {
        let peer = [3; 32];
        let mut pieces = PieceMap::new(1, 4, 8, 4).unwrap();
        let first = pieces.request_block(peer, 0, 0, 8, 16, 8).unwrap();
        let second = pieces.request_block(peer, 0, 8, 8, 16, 8).unwrap();
        assert_eq!(pieces.status(0), Some(PieceStatus::InFlight));

        pieces.complete_block(&first);
        assert_eq!(pieces.status(0), Some(PieceStatus::InFlight));
        pieces.complete_block(&second);
        assert_eq!(pieces.status(0), Some(PieceStatus::Missing));
        assert_eq!(pieces.request_block(peer, 0, 0, 8, 16, 8).unwrap(), first);
    }
}
