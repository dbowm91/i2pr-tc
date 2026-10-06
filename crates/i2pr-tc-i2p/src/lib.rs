//! I2P-only torrent transport adapters over an application-owned I2P session.
//!
//! This crate deliberately has no host-network connector. The application asks
//! [`sam::SamConnectionFactory`] for one raw SAM protocol connection at a time
//! and speaks SAM v3.3 over it, so this crate needs no router or gateway
//! vocabulary at all: its contract is "exact ordered protocol octets on a
//! stream this app asked for".
//!
//! [`sam::SamClient`] owns the session model SAM 3.3 defines for sharing one
//! I2P Destination across protocols: one long-lived primary session that owns
//! the identity and the tunnels, plus the STREAM, DATAGRAM, and RAW child
//! channels attached to it. See [`sam`] for the qualified wire behaviour and the
//! ownership rules.

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
    #[error("I2P identity is not established yet")]
    IdentityNotReady,
    #[error("I2P session generation is stale: {0}")]
    Stale(&'static str),
    #[error("SAM reply reported {0:?}")]
    SamReply(crate::sam::SamResult),
}

/// An application-owned I2P session over private SAM.
///
/// Implementations map these operations onto the session's own child channels.
/// They must not open a host SAM socket or expose router identity.
#[async_trait]
pub trait I2pSession: Send + Sync {
    /// The local Destination hash, as the router has confirmed it.
    ///
    /// A session that has not been established yet must return a typed
    /// not-ready error here rather than a placeholder hash: a derived or
    /// invented value would be trusted by every consumer that filters itself
    /// out of PEX, announces, and self-connection checks.
    fn local_peer_hash(&self) -> Result<[u8; 32], TransportError>;

    async fn lookup(&self, name: &str) -> Result<identity::Destination, TransportError>;

    async fn connect(
        &self,
        destination: &identity::Destination,
        port: u16,
    ) -> Result<I2pStream, TransportError>;

    async fn accept(&self) -> Result<(identity::Destination, I2pStream), TransportError>;
}
