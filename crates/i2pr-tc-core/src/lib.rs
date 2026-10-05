//! Bounded, transport-independent BitTorrent v1 domain primitives.
#![forbid(unsafe_code)]

pub mod bencode;
pub mod extension;
pub mod magnet;
pub mod metainfo;
pub mod service;
pub mod state;
pub mod wire;

pub use metainfo::{InfoHashV1, TorrentMeta};
