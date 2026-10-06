//! I2P-only torrent transport adapters over a router-owned injected session.
//!
//! This crate deliberately has no host-network connector. Managed runtime
//! composition supplies [`I2pSession`] and owns its SAM/I2CP lifecycle.
//!
//! [`sam`] implements that lifecycle directly: the application asks
//! [`sam::SamConnectionFactory`] for one raw SAM protocol byte stream and speaks
//! SAM v3 over it. No router or gateway type is named anywhere in this crate,
//! because the managed runtime's contract is "exact ordered protocol octets on a
//! stream this app asked for", which needs no router vocabulary at all.

pub mod identity;
pub mod metadata;
pub mod peer;
pub mod pex;
pub mod sam;
pub mod tracker;

use async_trait::async_trait;
use std::{io, pin::Pin};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

pub trait AsyncIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncIo for T {}
pub type I2pStream = Pin<Box<dyn AsyncIo>>;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("invalid I2P destination or address")]
    Address,
    #[error("router session failed: {0}")]
    Session(String),
    #[error("I2P stream I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("operation timed out")]
    Timeout,
    #[error("bounded protocol input was exceeded or malformed")]
    Protocol,
    #[error("I2P tracker rejected the announce: {0}")]
    Tracker(String),
    #[error("operation was cancelled")]
    Cancelled,
}

/// An application-scoped, router-owned I2P session adapter.
///
/// Implementations map these operations onto an existing private SAM/I2CP
/// connection. They must not open a host SAM socket or expose router identity.
#[async_trait]
pub trait I2pSession: Send + Sync {
    fn local_peer_hash(&self) -> [u8; 32];

    async fn lookup(&self, name: &str) -> Result<identity::Destination, TransportError>;

    async fn connect(
        &self,
        destination: &identity::Destination,
        port: u16,
    ) -> Result<I2pStream, TransportError>;

    async fn accept(&self) -> Result<(identity::Destination, I2pStream), TransportError>;
}
