//! Application-owned SAM v3.3 protocol client over raw SAM connections.
//!
//! The production boundary for this module is deliberately narrow: the managed
//! runtime asks for one fresh raw SAM protocol connection and this module
//! speaks SAM itself. The managed-app implementation of
//! [`SamConnectionFactory`] lives in the runtime composition layer and maps one
//! successful `open(service=sam)` to exactly one SAM protocol connection whose
//! ordered octets are the SAM octets written here, unmodified. No router type,
//! router credential, or router private key is named or required anywhere in
//! this module, and no host socket connector exists here.
//!
//! # Ownership
//!
//! SAM's shared-Destination model is one long-lived session that owns one I2P
//! Destination and one tunnel set, with protocol channels attached to it. This
//! module owns exactly that shape:
//!
//! ```text
//! TorrentI2pTransport
//!   |
//!   +-- one persistent primary control connection
//!   |     HELLO VERSION MIN=3.1 MAX=3.3
//!   |     SESSION CREATE STYLE=PRIMARY ID=<id> DESTINATION=<keys|TRANSIENT>
//!   |     stays owned for the primary session lifetime
//!   |
//!   +-- STREAM child      -> peer CONNECT/ACCEPT, HTTP tracker streams
//!   +-- DATAGRAM child    -> protocol 17 repliable transport, reserved for DHT
//!   +-- RAW child         -> protocol 18 transport, reserved for DHT
//! ```
//!
//! A child channel never issues `SESSION CREATE`: attaching one is a
//! `SESSION ADD` on the primary's own control connection, exactly as the SAM
//! 3.3 specification requires. Losing the control connection is a
//! transport-wide session event: every child channel of that generation becomes
//! stale and fails rather than silently attaching to a replacement identity.
//!
//! # Wire facts established against a real router
//!
//! Every one of these was qualified against i2pd 2.61.0, not read off a spec.
//!
//! * `SESSION CREATE` carries a mandatory `DESTINATION=` parameter; a real
//!   service answers the command without one with `INVALID_KEY`.
//! * Every base64 value uses I2P's substitution alphabet, in which `-` and `~`
//!   stand where the standard alphabet puts `+` and `/`. A standard-alphabet
//!   value is rejected rather than decoded.
//! * The reply keys differ per command: `NAMING REPLY` carries the Destination
//!   under `VALUE`, while `STREAM ACCEPT` reports the connecting peer as a bare
//!   line with no option key at all.
//! * A shared-Destination session is spelled `STYLE=PRIMARY` by the current
//!   specification and Java I2P; i2pd 2.61.0 answers `STYLE=PRIMARY` with
//!   `I2P_ERROR MESSAGE="Unknown STYLE"` and requires the legacy
//!   `STYLE=MASTER` spelling instead. Both are attempted, one per fresh
//!   connection, and the accepted spelling becomes recorded negotiated state.
//! * `SESSION ADD`/`SESSION REMOVE` are only accepted on the control connection
//!   that created the shared session, and only for a shared session.
//! * `SESSION ADD` with `STYLE=DATAGRAM` or `STYLE=RAW` needs a datagram
//!   routing port and, in both Java I2P and i2pd, its datagram delivery model is
//!   the bridge's own UDP port rather than the application socket. This crate
//!   therefore never requests a host datagram socket; see
//!   [`SamChannelConfig`].
//! * Datagram traffic is framed on the control connection itself
//!   (`DATAGRAM SEND` / `RAW DATA SEND` and their `RECEIVE` counterparts), so a
//!   managed application needs no host UDP authority to use them.
#![forbid(unsafe_code)]

use crate::{I2pSession, I2pStream, TransportError, identity};
use async_trait::async_trait;
use i2pr_tc_storage::Cancellation;
use std::{
    collections::VecDeque,
    future::Future,
    io,
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
};

#[cfg(test)]
mod tests;

/// Smallest Destination accepted by [`crate::identity::Destination`].
const MIN_DESTINATION_BYTES: usize = 387;
/// Largest Destination accepted by [`crate::identity::Destination`].
const MAX_DESTINATION_BYTES: usize = 8192;
/// The public-key-plus-signing-key prefix every Destination starts with: 256
/// bytes of public key followed by 128 bytes of signing key, which the
/// specification says are aligned at the start and the end respectively.
const DESTINATION_KEY_BYTES: usize = 384;
/// Bound on a command token (session identifier or naming lookup name).
const MAX_COMMAND_TOKEN_BYTES: usize = 512;
/// Bound on a `SESSION CREATE` `DESTINATION=` value. A real service's key
/// material runs to a few hundred bytes and the certificate length is its own
/// business; this bound exists only to keep one command line finite.
const MAX_SESSION_KEYS_TOKEN_BYTES: usize = 16 * 1024;
/// Bound on one datagram payload this client will send or accept.
///
/// The protocol-17 repliable form is bounded by the I2P datagram maximum; the
/// bound is the client's own input bound as well, so a router cannot make this
/// client buffer an unbounded "received" block.
const MAX_DATAGRAM_PAYLOAD_BYTES: usize = 32 * 1024;
/// I2CP protocol number for signed, repliable I2P datagrams.
pub const I2P_PROTOCOL_REPLIABLE: u8 = 17;
/// I2CP protocol number for unsigned raw I2P datagrams.
pub const I2P_PROTOCOL_RAW: u8 = 18;
/// Cancellation poll granularity for cancellable operations.
const CANCELLATION_POLL: Duration = Duration::from_millis(20);
/// Wait granularity while draining in-flight work during [`SamClient::close`].
const CLOSE_POLL: Duration = Duration::from_millis(10);
/// Commands the primary control connection will queue before a caller is told
/// the primary is applying backpressure rather than silently buffering more.
const PRIMARY_QUEUE_DEPTH: usize = 8;

/// One fresh raw SAM protocol connection, carrying exact ordered SAM octets.
///
/// This is deliberately the crate's existing boxed stream type so a stream
/// returned after SAM framing is consumed can be handed straight to tracker
/// and peer code without an adapter layer.
pub type SamRawStream = I2pStream;

/// Requests one fresh raw SAM protocol connection.
///
/// Exactly the ordered SAM octets written to the returned stream reach the
/// SAM service. There is no shared multiplexing connection and no gateway
/// protocol translation: one call yields one independent SAM protocol
/// connection.
#[async_trait]
pub trait SamConnectionFactory: Send + Sync {
    /// Requests one fresh raw SAM protocol connection.
    ///
    /// The managed-runtime implementation opens a `service=sam` logical stream;
    /// the gateway maps exactly one successful open to exactly one SAM protocol
    /// connection carrying exact ordered octets.
    async fn open_sam_connection(&self) -> Result<SamRawStream, TransportError>;
}

/// Bounds applied to every reply line, value, and option block.
///
/// The default is the safe, production-shaped bound; [`SamLimits::validate`]
/// rejects any combination that could not hold one option value on a line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SamLimits {
    pub max_line_bytes: usize,
    pub max_reply_bytes: usize,
    pub max_token_bytes: usize,
    pub max_value_bytes: usize,
    pub max_options: usize,
    pub max_datagram_bytes: usize,
}

impl Default for SamLimits {
    fn default() -> Self {
        Self {
            max_line_bytes: 16 * 1024,
            max_reply_bytes: 32 * 1024,
            max_token_bytes: 64,
            max_value_bytes: 12 * 1024,
            max_options: 8,
            max_datagram_bytes: MAX_DATAGRAM_PAYLOAD_BYTES,
        }
    }
}

impl SamLimits {
    /// A deliberately tight bound set for adversarial qualification.
    ///
    /// Every rejection path that the default bounds reach only for very large
    /// input is reachable with this profile, which is what makes the bounds
    /// testable and fuzzable rather than merely declared.
    pub const fn strict() -> Self {
        Self {
            max_line_bytes: 64,
            max_reply_bytes: 128,
            max_token_bytes: 16,
            max_value_bytes: 16,
            max_options: 4,
            max_datagram_bytes: 8,
        }
    }

    /// Rejects a bound set that cannot hold one option value on a line.
    pub fn validate(self) -> Result<(), SamError> {
        let smallest = self.max_token_bytes.checked_add(self.max_value_bytes);
        if self.max_token_bytes < 8
            || self.max_value_bytes < 8
            || self.max_options < 1
            || self.max_datagram_bytes < 1
            || smallest.is_none_or(|sum| self.max_line_bytes < sum + 1)
            || self.max_reply_bytes < self.max_line_bytes
        {
            return Err(SamError::Configuration);
        }
        Ok(())
    }
}

/// Per-operation deadlines. Every deadline is bounded and cancellable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SamTimeouts {
    /// Bound on acquiring one raw connection from the factory.
    pub open: Duration,
    /// Bound on the `HELLO` plus `SESSION CREATE` exchange per connection.
    pub handshake: Duration,
    /// Bound on a naming lookup exchange.
    pub lookup: Duration,
    /// Bound on a `STREAM CONNECT` exchange.
    pub connect: Duration,
    /// Bound on a `STREAM ACCEPT` exchange.
    pub accept: Duration,
    /// Bound on one datagram send or receive exchange.
    pub datagram: Duration,
    /// Bound on adding or removing one child channel.
    pub child: Duration,
    /// Bound on draining in-flight work during [`SamClient::close`].
    pub close: Duration,
}

impl Default for SamTimeouts {
    fn default() -> Self {
        Self {
            open: Duration::from_secs(30),
            handshake: Duration::from_secs(30),
            lookup: Duration::from_secs(60),
            connect: Duration::from_secs(120),
            accept: Duration::from_secs(300),
            datagram: Duration::from_secs(30),
            child: Duration::from_secs(30),
            close: Duration::from_secs(5),
        }
    }
}

impl SamTimeouts {
    /// Rejects a zero or unbounded deadline.
    pub fn validate(self) -> Result<(), SamError> {
        if [
            self.open,
            self.handshake,
            self.lookup,
            self.connect,
            self.accept,
            self.datagram,
            self.child,
            self.close,
        ]
        .iter()
        .any(|timeout| timeout.is_zero())
        {
            return Err(SamError::Configuration);
        }
        Ok(())
    }
}

/// Typed SAM protocol failures. No variant allocates on behalf of input.
#[derive(Debug, Error)]
pub enum SamError {
    #[error("invalid SAM configuration")]
    Configuration,
    #[error("bounded SAM input was exceeded")]
    Limit,
    #[error("malformed SAM reply: {0}")]
    Protocol(&'static str),
    #[error("SAM reply reported {0:?}")]
    Router(SamResult),
    #[error("SAM reply did not carry {0}")]
    Missing(&'static str),
    #[error("SAM connection I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("SAM operation timed out")]
    Timeout,
    #[error("SAM operation was cancelled")]
    Cancelled,
    #[error("SAM session is unavailable: {0}")]
    Unavailable(&'static str),
    #[error("SAM service does not offer the shared-Destination profile")]
    SharedSessionUnsupported,
    #[error("SAM child channel belongs to a replaced session generation")]
    StaleGeneration,
    #[error("SAM primary control connection is applying backpressure")]
    Backpressure,
}

impl From<SamError> for TransportError {
    fn from(error: SamError) -> Self {
        match error {
            SamError::Io(inner) => TransportError::Io(inner),
            SamError::Timeout => TransportError::Timeout,
            SamError::Cancelled => TransportError::Cancelled,
            SamError::StaleGeneration => {
                TransportError::Stale("sam session generation was replaced")
            }
            SamError::Backpressure => {
                TransportError::Session("sam primary control queue is full".to_owned())
            }
            SamError::Router(result) => TransportError::SamReply(result),
            SamError::Unavailable(reason) => TransportError::Session(reason.to_owned()),
            // A service that never offered the shared-Destination profile is a
            // session capability gap, not a malformed reply.
            SamError::SharedSessionUnsupported => TransportError::Session(
                "sam service does not offer the shared-Destination profile".to_owned(),
            ),
            SamError::Configuration
            | SamError::Limit
            | SamError::Protocol(_)
            | SamError::Missing(_) => TransportError::Protocol,
        }
    }
}

/// The SAM v3 result vocabulary this client recognises.
///
/// The specification's normative set is `OK`, `CANT_REACH_PEER`,
/// `DUPLICATED_DEST`, `I2P_ERROR`, `INVALID_KEY`, `KEY_NOT_FOUND`,
/// `PEER_NOT_FOUND`, `TIMEOUT`, and `LEASESET_NOT_FOUND`. `NOVERSION`,
/// `DUPLICATED_ID`, `INVALID_ID`, `INVALID_NAME`, `CONNECTION_REFUSED`, and
/// `NOT_IMPLEMENTED` are additions real bridges emit and are kept so their
/// failures stay typed rather than becoming "unknown result".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SamResult {
    Ok,
    NoVersion,
    InvalidKey,
    KeyNotFound,
    DuplicatedId,
    DuplicatedDestination,
    InvalidId,
    InvalidName,
    LeaseSetNotFound,
    Timeout,
    CantReachPeer,
    ConnectionRefused,
    NotImplemented,
    PeerNotFound,
    I2pError,
}

impl SamResult {
    fn parse(value: &str) -> Result<Self, SamError> {
        Ok(match value {
            "OK" => Self::Ok,
            "NOVERSION" => Self::NoVersion,
            "INVALID_KEY" => Self::InvalidKey,
            "KEY_NOT_FOUND" => Self::KeyNotFound,
            "DUPLICATED_ID" => Self::DuplicatedId,
            "DUPLICATED_DEST" | "DUPLICATED_DESTINATION" => Self::DuplicatedDestination,
            "INVALID_ID" => Self::InvalidId,
            "INVALID_NAME" => Self::InvalidName,
            "LEASESET_NOT_FOUND" => Self::LeaseSetNotFound,
            "TIMEOUT" => Self::Timeout,
            "CANT_REACH_PEER" => Self::CantReachPeer,
            "CONNECTION_REFUSED" => Self::ConnectionRefused,
            "NOT_IMPLEMENTED" => Self::NotImplemented,
            "PEER_NOT_FOUND" => Self::PeerNotFound,
            "I2P_ERROR" => Self::I2pError,
            _ => return Err(SamError::Protocol("unknown SAM result")),
        })
    }
}

/// The SAM reply families this client accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SamReplyKind {
    Hello,
    Session,
    Stream,
    Naming,
    Datagram,
    Raw,
}

/// The child transports attachable to one shared-Destination session.
///
/// This vocabulary is deliberately transport-neutral: adding a datagram form
/// later is a new variant and a new encoder, never a change to the primary
/// session owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SamChannelKind {
    /// Reliable, ordered, bidirectional virtual streams (I2CP protocol 6).
    Stream,
    /// Signed, repliable datagrams (I2CP protocol 17).
    RepliableDatagram,
    /// Unsigned raw datagrams (I2CP protocol 18).
    RawDatagram,
}

impl SamChannelKind {
    /// The exact `STYLE=` spelling `SESSION ADD` carries for this channel.
    pub const fn style(self) -> SamStyle {
        match self {
            Self::Stream => SamStyle::Stream,
            Self::RepliableDatagram => SamStyle::Datagram,
            Self::RawDatagram => SamStyle::Raw,
        }
    }

    /// The I2CP protocol number a router uses to route this channel.
    pub const fn protocol(self) -> u8 {
        match self {
            Self::Stream => 6,
            Self::RepliableDatagram => I2P_PROTOCOL_REPLIABLE,
            Self::RawDatagram => I2P_PROTOCOL_RAW,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::RepliableDatagram => "datagram",
            Self::RawDatagram => "raw",
        }
    }
}

/// A `SESSION` style value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SamStyle {
    Stream,
    Datagram,
    Raw,
    Datagram2,
    Datagram3,
}

impl SamStyle {
    fn wire(self) -> &'static str {
        match self {
            Self::Stream => "STREAM",
            Self::Datagram => "DATAGRAM",
            Self::Raw => "RAW",
            Self::Datagram2 => "DATAGRAM2",
            Self::Datagram3 => "DATAGRAM3",
        }
    }
}

/// The two spellings of the shared-Destination session style.
///
/// The current SAM 3.3 specification and Java I2P use `PRIMARY`. i2pd keeps
/// the older `MASTER` spelling and answers the current spelling with
/// `I2P_ERROR MESSAGE="Unknown STYLE"`. They are the same protocol model, so a
/// client may offer the normative spelling first and record which one the
/// service accepted; it must never send both on one connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SamPrimaryStyle {
    Primary,
    Master,
}

impl SamPrimaryStyle {
    /// The order in which a client offers the spellings. The normative
    /// spelling is always tried first, on its own fresh connection.
    pub const OFFER_ORDER: [Self; 2] = [Self::Primary, Self::Master];

    const fn wire(self) -> &'static str {
        match self {
            Self::Primary => "PRIMARY",
            Self::Master => "MASTER",
        }
    }
}

/// What a shared-Destination session negotiated on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SamNegotiated {
    /// Version the service selected from the `HELLO` range.
    pub version: SamVersion,
    /// Style spelling the service accepted for the shared session.
    pub primary_style: SamPrimaryStyle,
}

/// A parsed SAM reply block: one status line plus its `KEY=VALUE` lines.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SamReply {
    kind: SamReplyKind,
    words: Vec<String>,
    result: Option<SamResult>,
    /// A delivery form (`DATAGRAM RECEIVED`, `RAW DATA RECEIVED`) reports the
    /// packet, not a command outcome, so it carries no `RESULT`.
    delivery: bool,
    options: Vec<(String, String)>,
}

impl SamReply {
    pub fn kind(&self) -> SamReplyKind {
        self.kind
    }

    /// Whether this reply reports delivered packet data rather than a command
    /// outcome.
    pub fn is_delivery(&self) -> bool {
        self.delivery
    }

    pub fn result(&self) -> Option<SamResult> {
        self.result
    }

    /// The leading protocol words of the status line, for example
    /// `["DATAGRAM", "RECEIVED"]` or `["RAW", "DATA", "SEND"]`.
    pub fn words(&self) -> &[String] {
        &self.words
    }

    pub fn option(&self, key: &str) -> Option<&str> {
        self.options
            .iter()
            .find(|(existing, _)| existing == key)
            .map(|(_, value)| value.as_str())
    }

    /// Converts a non-`OK` result into a typed failure.
    pub fn require_ok(&self) -> Result<(), SamError> {
        match self.result {
            Some(SamResult::Ok) => Ok(()),
            Some(other) => Err(SamError::Router(other)),
            None => Err(SamError::Protocol("SAM reply is missing RESULT")),
        }
    }

    /// Reads an unsigned decimal option within an explicit range.
    pub fn number<T: SamNumber>(&self, key: &str) -> Result<T, SamError> {
        let raw = self
            .option(key)
            .ok_or(SamError::Missing("a required numeric SAM option"))?;
        T::parse_sam_number(raw)
    }

    /// Reads an unsigned decimal option, falling back to a default when the
    /// bridge omitted it. Ports and the raw protocol are optional in the
    /// datagram reply forms of older bridges.
    pub fn number_or<T: SamNumber>(&self, key: &str, default: T) -> Result<T, SamError> {
        match self.option(key) {
            Some(raw) => T::parse_sam_number(raw),
            None => Ok(default),
        }
    }

    /// Decodes a base64 Destination option into a verified Destination.
    pub fn destination(&self, key: &'static str) -> Result<identity::Destination, SamError> {
        decode_destination(self.option(key).ok_or(SamError::Missing(key))?)
    }
}

/// A bounded, allocation-free decimal option value.
pub trait SamNumber: Sized + Copy {
    fn from_decimal(value: u64) -> Option<Self>;
    fn parse_sam_number(raw: &str) -> Result<Self, SamError>;
}

impl SamNumber for u8 {
    fn from_decimal(value: u64) -> Option<Self> {
        u8::try_from(value).ok()
    }

    fn parse_sam_number(raw: &str) -> Result<Self, SamError> {
        u8::try_from(parse_number(raw)?)
            .map_err(|_| SamError::Protocol("SAM number is out of range"))
    }
}

impl SamNumber for u16 {
    fn from_decimal(value: u64) -> Option<Self> {
        u16::try_from(value).ok()
    }

    fn parse_sam_number(raw: &str) -> Result<Self, SamError> {
        u16::try_from(parse_number(raw)?)
            .map_err(|_| SamError::Protocol("SAM number is out of range"))
    }
}

impl SamNumber for u32 {
    fn from_decimal(value: u64) -> Option<Self> {
        u32::try_from(value).ok()
    }

    fn parse_sam_number(raw: &str) -> Result<Self, SamError> {
        u32::try_from(parse_number(raw)?)
            .map_err(|_| SamError::Protocol("SAM number is out of range"))
    }
}

fn parse_number(raw: &str) -> Result<u64, SamError> {
    if raw.is_empty() || raw.len() > 20 {
        return Err(SamError::Protocol("non-decimal SAM number"));
    }
    let mut value = 0u64;
    for digit in raw.bytes() {
        if !digit.is_ascii_digit() {
            return Err(SamError::Protocol("non-decimal SAM number"));
        }
        value = value
            .checked_mul(10)
            .and_then(|scaled| scaled.checked_add(u64::from(digit - b'0')))
            .ok_or(SamError::Protocol("overflowing SAM number"))?;
    }
    Ok(value)
}

/// Decodes and verifies one Destination carried as base64.
///
/// A value that is merely too large stays a limit violation; a value that is
/// not base64, is out of the Destination length range, or is not structurally a
/// Destination is a protocol violation. The structural check is what stops a
/// private-key blob — which a real bridge returns in the same `DESTINATION=`
/// option for a transient session, as 663 or more bytes — from being mistaken
/// for a Destination and cached as the local identity.
///
/// The layout a Destination has is 384 key bytes followed by a certificate of
/// at least three bytes: 256 bytes of public key, then 128 bytes of signing
/// key, then a one-byte certificate type, a two-byte big-endian payload
/// length, and that many payload bytes. Keys come first, and the two length
/// bytes must account for the value's length exactly.
pub fn decode_destination(value: &str) -> Result<identity::Destination, SamError> {
    let bytes = match decode_base64(value.as_bytes(), MAX_DESTINATION_BYTES) {
        Ok(bytes) => bytes,
        Err(SamError::Limit) => return Err(SamError::Limit),
        Err(_) => return Err(SamError::Protocol("SAM destination is not decodable")),
    };
    if !(MIN_DESTINATION_BYTES..=MAX_DESTINATION_BYTES).contains(&bytes.len())
        || !is_destination_structure(&bytes)
    {
        return Err(SamError::Protocol("SAM destination is not addressable"));
    }
    identity::Destination::from_bytes(bytes)
        .map_err(|_| SamError::Protocol("SAM destination is not addressable"))
}

/// Splits a decoded value into the parts of an I2P Destination, if it is one.
///
/// A Destination is 384 key bytes followed by a certificate of at least three
/// bytes: a 256-byte public key, then a 128-byte signing key, then a one-byte
/// certificate type and a two-byte big-endian certificate payload length, then
/// that many payload bytes. Keys come *first*. This is worth stating precisely
/// because a certificate-first reading is the obvious mistake, and a router
/// running i2pd 2.61.0 answers every `NAMING LOOKUP` with exactly this
/// keys-first layout.
fn destination_parts(bytes: &[u8]) -> Option<(usize, &[u8])> {
    let header = bytes.get(DESTINATION_KEY_BYTES..DESTINATION_KEY_BYTES + 3)?;
    // A certificate length is a two-byte big-endian integer; a type byte that
    // claims an impossible payload length is not a Destination.
    let payload_length = usize::from(u16::from_be_bytes([header[1], header[2]]));
    bytes
        .get(DESTINATION_KEY_BYTES + 3 + payload_length..)
        .or(Some(&[]))
        .filter(|_| bytes.len() == DESTINATION_KEY_BYTES + 3 + payload_length)
        .map(|payload| (DESTINATION_KEY_BYTES, payload))
}

/// Whether a decoded value is structurally one Destination rather than a
/// private-key blob that merely starts with a Destination.
fn is_destination_structure(bytes: &[u8]) -> bool {
    destination_parts(bytes).is_some()
}

/// A bounded, allocation-capped SAM version number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SamVersion {
    pub major: u32,
    pub minor: u32,
}

impl SamVersion {
    pub fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    /// Parses `MAJOR.MINOR`, rejecting non-decimal or overflowing components.
    pub fn parse(text: &str) -> Result<Self, SamError> {
        if text.is_empty() || text.len() > 15 {
            return Err(SamError::Protocol("malformed SAM version"));
        }
        let (major, minor) = text
            .split_once('.')
            .ok_or(SamError::Protocol("malformed SAM version"))?;
        if minor.contains('.') {
            return Err(SamError::Protocol("malformed SAM version"));
        }
        let major = u32::try_from(parse_number(major)?)
            .map_err(|_| SamError::Protocol("overflowing SAM version"))?;
        let minor = u32::try_from(parse_number(minor)?)
            .map_err(|_| SamError::Protocol("overflowing SAM version"))?;
        Ok(Self { major, minor })
    }

    fn within(self, min: Self, max: Self) -> bool {
        self >= min && self <= max
    }
}

impl std::fmt::Display for SamVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}", self.major, self.minor)
    }
}

/// Encodes `bytes` as padded I2P base64.
///
/// I2P substitutes `-` and `~` for the two standard base64 symbols at index
/// 62 and 63 and pads with `=`. Every real router speaks only that form: a
/// standard-alphabet value is rejected outright rather than decoded, so this
/// alphabet is the wire format rather than a cosmetic variant of it.
pub fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-~";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = u32::from(chunk[0]);
        let second = chunk.get(1).copied().map_or(0, u32::from);
        let third = chunk.get(2).copied().map_or(0, u32::from);
        let group = (first << 16) | (second << 8) | third;
        out.push(ALPHABET[(group >> 18 & 63) as usize] as char);
        out.push(ALPHABET[(group >> 12 & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(group >> 6 & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(group & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Decodes padded I2P base64, rejecting non-alphabet bytes, bad padding, and
/// inputs that could exceed `max_bytes`.
///
/// Length must stay a multiple of four: that is what a real service's own
/// decoder requires, so an unpadded value is malformed here rather than
/// silently accepted on the assumption that some router emits one.
pub fn decode_base64(input: &[u8], max_bytes: usize) -> Result<Vec<u8>, SamError> {
    if input.len() > max_bytes.div_ceil(3) * 4 {
        return Err(SamError::Limit);
    }
    if input.len() % 4 != 0 {
        return Err(SamError::Protocol("base64 input is not quad aligned"));
    }
    let mut padding = 0usize;
    while padding < input.len() && input[input.len() - 1 - padding] == b'=' {
        padding += 1;
    }
    if padding > 2 {
        return Err(SamError::Protocol("invalid base64 padding"));
    }
    let body = &input[..input.len() - padding];
    if body.contains(&b'=') {
        return Err(SamError::Protocol("misplaced base64 padding"));
    }
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    let (mut accumulator, mut bits) = (0u32, 0u32);
    for byte in body {
        let value = base64_value(*byte).ok_or(SamError::Protocol("non-alphabet base64 byte"))?;
        accumulator = (accumulator << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            if out.len() >= max_bytes {
                return Err(SamError::Limit);
            }
            out.push((accumulator >> bits) as u8);
        }
    }
    Ok(out)
}

fn base64_value(byte: u8) -> Option<u32> {
    Some(match byte {
        b'A'..=b'Z' => u32::from(byte - b'A'),
        b'a'..=b'z' => u32::from(byte - b'a') + 26,
        b'0'..=b'9' => u32::from(byte - b'0') + 52,
        // Index 62 and 63, in I2P's substitution rather than the standard one.
        // `+` and `/` are deliberately absent: a real service does not decode
        // them, so accepting them would only hide a broken peer.
        b'-' => 62,
        b'~' => 63,
        _ => return None,
    })
}

/// Exact `HELLO VERSION` command octets.
pub fn encode_hello(min: SamVersion, max: SamVersion) -> Result<Vec<u8>, SamError> {
    Ok(format!("HELLO VERSION MIN={min} MAX={max}\n").into_bytes())
}

/// What `SESSION CREATE` asks the SAM service to use as the session identity.
///
/// The parameter is mandatory, not optional: a real service answers a command
/// that omits it with `INVALID_KEY`, so this client can never emit one. Only
/// these two forms exist on the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SamSessionDestination {
    /// `DESTINATION=TRANSIENT`: the service mints a fresh ephemeral identity
    /// for this session and returns it. The identity is therefore gone when the
    /// session goes, and a restored transport is a different node.
    Transient,
    /// `DESTINATION=<base64 private keys>`: the service adopts injected key
    /// material, so one identity outlives any single connection.
    ///
    /// The exact shape of that material is the service's contract, not this
    /// crate's: real services disagree about it, and a value one accepts is
    /// not necessarily well formed for the other. This client therefore
    /// checks only that the value can be carried on a command line and
    /// decoded as base64, and leaves the rest to the `INVALID_KEY` that a
    /// service returns for key material it will not take.
    ///
    /// Where that material is persisted is not this crate's decision: the
    /// managed runtime composition owns that secret.
    PrivateKeys(String),
}

impl SamSessionDestination {
    /// Rejects key material that could not be carried as one command value.
    pub fn validate(&self) -> Result<(), SamError> {
        let SamSessionDestination::PrivateKeys(encoded) = self else {
            return Ok(());
        };
        if encoded.is_empty() || encoded.len() > MAX_SESSION_KEYS_TOKEN_BYTES {
            return Err(SamError::Limit);
        }
        // `=` is permitted here even though a bare session identifier may not
        // contain it: this is the value half of a `KEY=VALUE` pair, and base64
        // padding is a legitimate character there.
        if !encoded.is_ascii()
            || encoded
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || matches!(byte, b'"' | b'\\'))
        {
            return Err(SamError::Protocol("SAM private keys are not a bare value"));
        }
        decode_base64(encoded.as_bytes(), MAX_SESSION_KEYS_TOKEN_BYTES / 4 * 3)?;
        Ok(())
    }
}

/// The application torrent identity and its single bounded SAM session id.
///
/// The private destination is an injected value: this type never reads a
/// filesystem secret, environment variable, or any other ambient authority.
/// Deciding whether and where destination key material is persisted belongs to
/// the managed runtime composition layer.
///
/// This type deliberately holds no local hash. A hash here could only be
/// derived from the session identifier, which is a claim the router has never
/// confirmed; the real local Destination hash is available only after the
/// router answers for it, and is reported by [`SamClient::local_identity`].
#[derive(Clone, Debug)]
pub struct SamIdentity {
    session_destination: SamSessionDestination,
    session_id: String,
}

impl SamIdentity {
    /// Builds an identity from the destination `SESSION CREATE` will request.
    pub fn new(
        session_destination: SamSessionDestination,
        session_id: &str,
    ) -> Result<Self, SamError> {
        validate_token(session_id)?;
        session_destination.validate()?;
        Ok(Self {
            session_destination,
            session_id: session_id.to_owned(),
        })
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// The destination form this client asks the service to bind the session to.
    pub fn session_destination(&self) -> &SamSessionDestination {
        &self.session_destination
    }

    /// Whether persistent private keys were injected rather than a transient
    /// identity requested from the service.
    ///
    /// A transient identity is per primary session: it cannot be dialled by a
    /// peer and it cannot survive a transport restart. A production profile
    /// needs the persistent form; only tests want the transient one.
    pub fn has_persistent_keys(&self) -> bool {
        matches!(
            self.session_destination,
            SamSessionDestination::PrivateKeys(_)
        )
    }
}

/// Exact `SESSION CREATE STYLE=PRIMARY` (or the legacy `MASTER`) octets.
///
/// The `DESTINATION=` parameter is always present: a real service rejects the
/// command without one, so omitting it is never a shorter spelling of the same
/// request. No data-port option is ever emitted: the specification forbids
/// `PORT`, `HOST`, `FROM_PORT`, `TO_PORT`, `PROTOCOL`, `LISTEN_PORT`,
/// `LISTEN_PROTOCOL`, and `HEADER` on a shared-Destination session.
pub fn encode_session_create_primary(
    session_id: &str,
    destination: &SamSessionDestination,
    style: SamPrimaryStyle,
) -> Result<Vec<u8>, SamError> {
    validate_token(session_id)?;
    destination.validate()?;
    let destination = match destination {
        SamSessionDestination::Transient => "TRANSIENT",
        SamSessionDestination::PrivateKeys(encoded) => encoded.as_str(),
    };
    Ok(format!(
        "SESSION CREATE STYLE={} ID={session_id} DESTINATION={destination}\n",
        style.wire()
    )
    .into_bytes())
}

/// Ports one child channel owns on the shared Destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SamChannelConfig {
    /// Local I2P port this client sends from.
    pub from_port: u16,
    /// Remote I2P port this client addresses.
    pub to_port: u16,
    /// Local I2P port the router delivers inbound traffic for this channel to.
    ///
    /// Explicit for every datagram channel even when it equals `from_port`,
    /// because inbound routing is ambiguous between two datagram channels that
    /// share one Destination unless their listen ports differ.
    pub listen_port: u16,
}

impl SamChannelConfig {
    /// The streaming shape: streaming channels carry no port options at all.
    pub const STREAM: Self = Self {
        from_port: 0,
        to_port: 0,
        listen_port: 0,
    };
}

/// Exact `SESSION ADD` octets for one child channel.
///
/// The style spelling is the channel's own style, never the shared session's.
/// `DESTINATION=` is deliberately never emitted: the child inherits the
/// primary's Destination, which is the entire point of the model.
pub fn encode_session_add(
    kind: SamChannelKind,
    child_id: &str,
    config: SamChannelConfig,
) -> Result<Vec<u8>, SamError> {
    validate_token(child_id)?;
    Ok(match kind {
        SamChannelKind::Stream => {
            format!("SESSION ADD STYLE={} ID={child_id}\n", SamStyle::Stream.wire())
        }
        SamChannelKind::RepliableDatagram => format!(
            "SESSION ADD STYLE={} ID={child_id} FROM_PORT={} TO_PORT={} LISTEN_PORT={}\n",
            SamStyle::Datagram.wire(),
            config.from_port,
            config.to_port,
            config.listen_port
        ),
        SamChannelKind::RawDatagram => format!(
            "SESSION ADD STYLE={} ID={child_id} FROM_PORT={} TO_PORT={} LISTEN_PORT={} PROTOCOL={}\n",
            SamStyle::Raw.wire(),
            config.from_port,
            config.to_port,
            config.listen_port,
            I2P_PROTOCOL_RAW
        ),
    }
    .into_bytes())
}

/// Exact `SESSION REMOVE` octets. No other option is valid on this command.
pub fn encode_session_remove(child_id: &str) -> Result<Vec<u8>, SamError> {
    validate_token(child_id)?;
    Ok(format!("SESSION REMOVE ID={child_id}\n").into_bytes())
}

/// Exact `NAMING LOOKUP` command octets.
pub fn encode_naming_lookup(name: &str) -> Result<Vec<u8>, SamError> {
    validate_token(name)?;
    Ok(format!("NAMING LOOKUP NAME={name}\n").into_bytes())
}

/// Exact `STREAM CONNECT` command octets, including an explicit port.
pub fn encode_stream_connect(
    child_id: &str,
    destination: &[u8],
    port: u16,
) -> Result<Vec<u8>, SamError> {
    validate_token(child_id)?;
    if destination.len() > MAX_DESTINATION_BYTES {
        return Err(SamError::Limit);
    }
    Ok(format!(
        "STREAM CONNECT ID={child_id} DESTINATION={} PORT={port} SILENT=false\n",
        encode_base64(destination)
    )
    .into_bytes())
}

/// Exact `STREAM ACCEPT` command octets.
pub fn encode_stream_accept(child_id: &str) -> Result<Vec<u8>, SamError> {
    validate_token(child_id)?;
    Ok(format!("STREAM ACCEPT ID={child_id} SILENT=false\n").into_bytes())
}

/// Exact `DATAGRAM SEND` octets: one command line, then exactly `SIZE` bytes.
pub fn encode_datagram_send(
    child_id: &str,
    destination: &[u8],
    config: SamChannelConfig,
    payload: &[u8],
) -> Result<Vec<u8>, SamError> {
    validate_token(child_id)?;
    if destination.len() > MAX_DESTINATION_BYTES {
        return Err(SamError::Limit);
    }
    if payload.is_empty() || payload.len() > MAX_DATAGRAM_PAYLOAD_BYTES {
        return Err(SamError::Limit);
    }
    let mut out = format!(
        "DATAGRAM SEND ID={child_id} DESTINATION={} FROM_PORT={} TO_PORT={} SIZE={}\n",
        encode_base64(destination),
        config.from_port,
        config.to_port,
        payload.len()
    )
    .into_bytes();
    out.extend_from_slice(payload);
    Ok(out)
}

/// Exact `DATAGRAM RECEIVE` octets. The bridge answers with
/// `DATAGRAM RECEIVED ... SIZE=n` followed by exactly `n` bytes.
pub fn encode_datagram_receive(child_id: &str) -> Result<Vec<u8>, SamError> {
    validate_token(child_id)?;
    Ok(format!("DATAGRAM RECEIVE ID={child_id}\n").into_bytes())
}

/// Exact `RAW DATA SEND` octets: one command line, then exactly `SIZE` bytes.
pub fn encode_raw_send(
    child_id: &str,
    destination: &[u8],
    config: SamChannelConfig,
    payload: &[u8],
) -> Result<Vec<u8>, SamError> {
    validate_token(child_id)?;
    if destination.len() > MAX_DESTINATION_BYTES {
        return Err(SamError::Limit);
    }
    if payload.is_empty() || payload.len() > MAX_DATAGRAM_PAYLOAD_BYTES {
        return Err(SamError::Limit);
    }
    let mut out = format!(
        "RAW DATA SEND ID={child_id} DESTINATION={} FROM_PORT={} TO_PORT={} PROTOCOL={} SIZE={}\n",
        encode_base64(destination),
        config.from_port,
        config.to_port,
        I2P_PROTOCOL_RAW,
        payload.len()
    )
    .into_bytes();
    out.extend_from_slice(payload);
    Ok(out)
}

/// Exact `RAW DATA RECEIVE` octets. The bridge answers with
/// `RAW DATA RECEIVED RESULT=OK ... SIZE=n` followed by exactly `n` bytes.
pub fn encode_raw_receive(child_id: &str) -> Result<Vec<u8>, SamError> {
    validate_token(child_id)?;
    Ok(format!("RAW DATA RECEIVE ID={child_id}\n").into_bytes())
}

fn validate_token(token: &str) -> Result<(), SamError> {
    if token.is_empty() || token.len() > MAX_COMMAND_TOKEN_BYTES {
        return Err(SamError::Limit);
    }
    if token
        .bytes()
        .any(|byte| byte.is_ascii_whitespace() || matches!(byte, b'"' | b'\\' | b'='))
        || !token.is_ascii()
    {
        return Err(SamError::Protocol("SAM command token is not a bare token"));
    }
    Ok(())
}

/// Parses one complete SAM reply block.
///
/// Pure and host-input agnostic: it performs no I/O, allocates only within
/// `limits`, and rejects malformed input with a typed error. A fuzz target can
/// drive it directly.
pub fn parse_reply_block(input: &[u8], limits: &SamLimits) -> Result<SamReply, SamError> {
    if input.len() > limits.max_reply_bytes {
        return Err(SamError::Limit);
    }
    let mut lines = input.split(|byte| *byte == b'\n');
    let first = lines
        .next()
        .ok_or(SamError::Protocol("empty SAM reply block"))?;
    let mut reply = parse_status_line(trim_ending(first), limits)?;
    let mut remaining: VecDeque<&[u8]> = lines.collect();
    while let Some(line) = remaining.pop_front() {
        if line.is_empty() && remaining.is_empty() {
            break;
        }
        let line = trim_ending(line);
        if line.is_empty() {
            return Err(SamError::Protocol("blank SAM reply line"));
        }
        let text = std::str::from_utf8(line)
            .map_err(|_| SamError::Protocol("SAM reply line is not UTF-8"))?;
        apply_token(&mut reply, text, limits)?;
    }
    if reply.result.is_none() && !reply.delivery {
        return Err(SamError::Protocol("SAM reply is missing RESULT"));
    }
    Ok(reply)
}

/// Splits one SAM line into whitespace-separated tokens.
///
/// A quoted value may contain spaces, which a real service uses for messages
/// such as `MESSAGE="Unknown STYLE"`, so splitting on every space would read
/// one quoted value as two malformed tokens.
fn split_tokens(line: &str, max_tokens: usize) -> Result<Vec<&str>, SamError> {
    let bytes = line.as_bytes();
    let mut tokens: Vec<&str> = Vec::new();
    let mut start = 0usize;
    let mut in_quotes = false;
    let mut escaped = false;
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        if escaped {
            escaped = false;
        } else if in_quotes {
            match byte {
                b'\\' => escaped = true,
                b'"' => in_quotes = false,
                _ => {}
            }
        } else {
            match byte {
                b'"' => in_quotes = true,
                b' ' => {
                    if index > start {
                        tokens.push(&line[start..index]);
                        if tokens.len() > max_tokens {
                            return Err(SamError::Limit);
                        }
                    }
                    start = index + 1;
                }
                _ => {}
            }
        }
        index += 1;
    }
    if in_quotes {
        return Err(SamError::Protocol("unterminated quoted SAM value"));
    }
    if index > start {
        tokens.push(&line[start..index]);
        if tokens.len() > max_tokens {
            return Err(SamError::Limit);
        }
    }
    Ok(tokens)
}

fn parse_status_line(line: &[u8], limits: &SamLimits) -> Result<SamReply, SamError> {
    if line.len() > limits.max_line_bytes {
        return Err(SamError::Limit);
    }
    let text = std::str::from_utf8(line)
        .map_err(|_| SamError::Protocol("SAM status line is not UTF-8"))?;
    // A SAM status line is up to three protocol words followed by `KEY=VALUE`
    // options: `HELLO REPLY ...`, `DATAGRAM RECEIVED ...`, `RAW DATA SEND ...`.
    let tokens = split_tokens(text, limits.max_options.saturating_add(3))?;
    let mut word_count = 0usize;
    while word_count < 3
        && tokens
            .get(word_count)
            .is_some_and(|token| !token.contains('='))
    {
        word_count += 1;
    }
    let words: Vec<String> = tokens[..word_count]
        .iter()
        .map(|token| (*token).to_owned())
        .collect();
    let (kind, delivery) = match words.as_slice() {
        [command, keyword] => match (command.as_str(), keyword.as_str()) {
            ("HELLO", "REPLY") => (SamReplyKind::Hello, false),
            ("NAMING", "REPLY") => (SamReplyKind::Naming, false),
            ("SESSION", "STATUS") => (SamReplyKind::Session, false),
            ("STREAM", "STATUS") => (SamReplyKind::Stream, false),
            ("DATAGRAM", "SEND") => (SamReplyKind::Datagram, false),
            ("DATAGRAM", "RECEIVED") => (SamReplyKind::Datagram, true),
            _ => return Err(SamError::Protocol("unknown SAM reply kind")),
        },
        [command, middle, third] if middle == "DATA" => match (command.as_str(), third.as_str()) {
            ("RAW", "SEND" | "RECEIVE") => (SamReplyKind::Raw, false),
            ("RAW", "RECEIVED") => (SamReplyKind::Raw, true),
            _ => return Err(SamError::Protocol("unknown SAM reply kind")),
        },
        _ => return Err(SamError::Protocol("unknown SAM reply kind")),
    };
    let mut reply = SamReply {
        kind,
        words,
        result: None,
        delivery,
        options: Vec::new(),
    };
    for token in &tokens[word_count..] {
        apply_token(&mut reply, token, limits)?;
    }
    Ok(reply)
}

fn apply_token(reply: &mut SamReply, token: &str, limits: &SamLimits) -> Result<(), SamError> {
    if token.len() > limits.max_line_bytes {
        return Err(SamError::Limit);
    }
    let (key, raw) = token
        .split_once('=')
        .ok_or(SamError::Protocol("SAM option is missing a value"))?;
    validate_key(key, limits)?;
    if key == "RESULT" {
        if reply.result.is_some() {
            return Err(SamError::Protocol("duplicate SAM RESULT"));
        }
        reply.result = Some(SamResult::parse(&decode_value(raw)?)?);
        return Ok(());
    }
    let value = decode_value(raw)?;
    if value.len() > limits.max_value_bytes {
        return Err(SamError::Limit);
    }
    if reply.options.len() >= limits.max_options {
        return Err(SamError::Limit);
    }
    if reply.options.iter().any(|(existing, _)| existing == key) {
        return Err(SamError::Protocol("duplicate SAM option"));
    }
    reply.options.push((key.to_owned(), value));
    Ok(())
}

fn validate_key(key: &str, limits: &SamLimits) -> Result<(), SamError> {
    if key.is_empty() {
        return Err(SamError::Protocol("SAM option key is empty"));
    }
    if key.len() > limits.max_token_bytes {
        return Err(SamError::Limit);
    }
    if !key
        .bytes()
        .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(SamError::Protocol(
            "SAM option key is not an uppercase token",
        ));
    }
    Ok(())
}

fn decode_value(raw: &str) -> Result<String, SamError> {
    let Some(body) = raw.strip_prefix('"') else {
        if raw.contains([' ', '\t', '"', '\\'])
            || raw.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
        {
            return Err(SamError::Protocol(
                "unquoted SAM value must not contain whitespace, quotes or escapes",
            ));
        }
        return Ok(raw.to_owned());
    };
    let mut out = Vec::with_capacity(body.len());
    let mut bytes = body.bytes();
    while let Some(byte) = bytes.next() {
        match byte {
            b'"' => {
                if bytes.next().is_some() {
                    return Err(SamError::Protocol(
                        "trailing bytes after a quoted SAM value",
                    ));
                }
                return String::from_utf8(out)
                    .map_err(|_| SamError::Protocol("SAM value is not UTF-8"));
            }
            b'\\' => match bytes.next() {
                Some(b'\\') => out.push(b'\\'),
                Some(b'"') => out.push(b'"'),
                _ => return Err(SamError::Protocol("unsupported SAM value escape")),
            },
            byte if byte < 0x20 || byte == 0x7f => {
                return Err(SamError::Protocol("control byte in a SAM value"));
            }
            byte => out.push(byte),
        }
    }
    Err(SamError::Protocol("unterminated quoted SAM value"))
}

fn trim_ending(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
}

async fn read_line(
    stream: &mut SamRawStream,
    limits: &SamLimits,
    budget: &mut usize,
) -> Result<Vec<u8>, SamError> {
    let mut line = Vec::with_capacity(limits.max_line_bytes.min(256));
    let mut byte = [0u8; 1];
    loop {
        if stream.read(&mut byte).await? == 0 {
            return Err(SamError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "sam connection ended before a complete reply",
            )));
        }
        *budget = budget.checked_sub(1).ok_or(SamError::Limit)?;
        line.push(byte[0]);
        if byte[0] == b'\n' {
            return Ok(line);
        }
        if line.len() > limits.max_line_bytes {
            return Err(SamError::Limit);
        }
    }
}

/// The router-confirmed local identity of one primary session.
///
/// Both fields are the router's answer, never a local derivation: the
/// Destination bytes come from the session's own `SESSION CREATE` reply or from
/// `NAMING LOOKUP NAME=ME`, and the hash is the SHA-256 of exactly those bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SamLocalIdentity {
    destination: identity::Destination,
}

impl SamLocalIdentity {
    /// The local Destination the router confirmed for this session.
    pub fn destination(&self) -> &identity::Destination {
        &self.destination
    }

    /// The SHA-256 Destination hash every consumer must use for self.
    pub fn hash(&self) -> [u8; 32] {
        self.destination.hash()
    }
}

/// One attached child channel of the live primary session.
///
/// A channel carries the generation it was added in. Once the primary is
/// replaced, every channel of the previous generation is stale and fails typed
/// rather than attaching to a new identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SamChannel {
    id: String,
    kind: SamChannelKind,
    generation: u64,
}

impl SamChannel {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn kind(&self) -> SamChannelKind {
        self.kind
    }

    /// The primary generation this channel belongs to.
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// A datagram the router delivered for a child channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SamDatagram {
    /// The authenticated sender, when the channel's datagram form supplies one.
    pub sender: Option<identity::Destination>,
    pub payload: Vec<u8>,
    pub from_port: u16,
    pub to_port: u16,
}

/// A raw datagram the router delivered for a child channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SamRawDatagram {
    pub payload: Vec<u8>,
    pub protocol: u8,
    pub from_port: u16,
    pub to_port: u16,
    /// Present only when the raw form's header option supplied a sender.
    pub sender: Option<identity::Destination>,
}

struct PrimaryCommand {
    octets: Vec<u8>,
    want: Option<&'static str>,
    reply: oneshot::Sender<Result<SamReply, TransportError>>,
}

#[derive(Debug)]
struct LivePrimary {
    generation: u64,
    negotiated: SamNegotiated,
    local: SamLocalIdentity,
    commands: mpsc::Sender<PrimaryCommand>,
    /// Held so the supervisor's lifetime is tied to the primary's, not to the
    /// first await that happens to touch the session.
    _supervisor: tokio::task::JoinHandle<()>,
    channels: usize,
}

#[derive(Debug, Default)]
struct PrimaryState {
    available: bool,
    live: Option<LivePrimary>,
    in_flight: usize,
}

/// Owner of one application torrent SAM primary session and its children.
///
/// The client owns exactly one long-lived primary control connection and the
/// lifecycle of every raw connection that attaches to it. Primary creation
/// happens exactly once per active generation and is an explicit bounded
/// transition: nothing here retries on its own, so an unavailable SAM service
/// cannot produce a reconnect storm. Ordinary connection loss on a child
/// connection does not disturb the primary, and loss of the primary makes every
/// child of that generation stale.
pub struct SamClient<F: SamConnectionFactory> {
    factory: F,
    identity: SamIdentity,
    limits: SamLimits,
    timeouts: SamTimeouts,
    min_version: SamVersion,
    max_version: SamVersion,
    shutdown: Cancellation,
    state: Arc<Mutex<PrimaryState>>,
    generation: Arc<std::sync::atomic::AtomicU64>,
    child_counters: [std::sync::atomic::AtomicU64; 3],
    channel_cache: Mutex<std::collections::HashMap<u64, SamChannel>>,
}

impl<F: SamConnectionFactory> SamClient<F> {
    /// Oldest SAM version this client negotiates.
    pub const MIN_VERSION: SamVersion = SamVersion { major: 3, minor: 1 };
    /// Newest SAM version this client negotiates.
    pub const MAX_VERSION: SamVersion = SamVersion { major: 3, minor: 3 };

    /// Builds a client over `factory` for one injected application identity.
    pub fn new(
        factory: F,
        identity: SamIdentity,
        limits: SamLimits,
        timeouts: SamTimeouts,
    ) -> Result<Self, SamError> {
        limits.validate()?;
        timeouts.validate()?;
        Ok(Self {
            factory,
            identity,
            limits,
            timeouts,
            min_version: Self::MIN_VERSION,
            max_version: Self::MAX_VERSION,
            shutdown: Cancellation::default(),
            state: Arc::new(Mutex::new(PrimaryState {
                available: true,
                ..PrimaryState::default()
            })),
            generation: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            child_counters: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
            channel_cache: Mutex::new(std::collections::HashMap::new()),
        })
    }

    pub fn identity(&self) -> &SamIdentity {
        &self.identity
    }

    pub fn session_id(&self) -> &str {
        self.identity.session_id()
    }

    pub fn session_destination(&self) -> &SamSessionDestination {
        self.identity.session_destination()
    }

    pub fn limits(&self) -> SamLimits {
        self.limits
    }

    pub fn timeouts(&self) -> SamTimeouts {
        self.timeouts
    }

    /// Whether dependent transport operations may still be attempted.
    pub fn is_available(&self) -> bool {
        self.lock().map(|state| state.available).unwrap_or(false)
    }

    /// Whether a primary session is currently live.
    pub fn has_primary(&self) -> bool {
        self.lock()
            .map(|state| state.live.is_some())
            .unwrap_or(false)
    }

    /// The live primary's generation, or zero when none is live.
    pub fn generation(&self) -> u64 {
        self.lock()
            .ok()
            .and_then(|state| state.live.as_ref().map(|live| live.generation))
            .unwrap_or(0)
    }

    /// What the live primary negotiated, if one is live.
    pub fn negotiated(&self) -> Option<SamNegotiated> {
        self.lock()
            .ok()
            .and_then(|state| state.live.as_ref().map(|live| live.negotiated))
    }

    /// Number of child channels currently attached to the live primary.
    pub fn channel_count(&self) -> usize {
        self.lock()
            .ok()
            .and_then(|state| state.live.as_ref().map(|live| live.channels))
            .unwrap_or(0)
    }

    /// Number of operations currently holding a raw connection.
    pub fn in_flight(&self) -> usize {
        self.lock().map(|state| state.in_flight).unwrap_or(0)
    }

    /// Whether [`SamClient::close`] or a drop has cancelled this client.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    /// The router-confirmed local Destination and its SHA-256 hash.
    ///
    /// This returns a typed not-ready error rather than a placeholder whenever
    /// no primary session has answered for the identity yet.
    pub fn local_identity(&self) -> Result<SamLocalIdentity, TransportError> {
        self.lock()?
            .live
            .as_ref()
            .map(|live| live.local.clone())
            .ok_or(TransportError::IdentityNotReady)
    }

    /// The router-confirmed local Destination hash.
    pub fn local_destination_hash(&self) -> Result<[u8; 32], TransportError> {
        self.local_identity().map(|identity| identity.hash())
    }

    /// Marks the transport unavailable and cancels in-flight work.
    ///
    /// This is the failure path a caller takes when it learns the SAM service is
    /// gone; it never destroys the injected identity, and it never retries.
    pub fn mark_unavailable(&self) {
        if let Ok(mut state) = self.lock() {
            state.available = false;
            state.live = None;
        }
        self.shutdown.cancel();
    }

    /// Cancels in-flight work, marks the transport unavailable, and waits a
    /// bounded time for outstanding operations to unwind.
    pub async fn close(&self) -> Result<(), TransportError> {
        self.mark_unavailable();
        let deadline = tokio::time::Instant::now() + self.timeouts.close;
        loop {
            if self.in_flight() == 0 {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(TransportError::Timeout);
            }
            tokio::time::sleep(CLOSE_POLL).await;
        }
    }

    /// Creates the primary session if none is live, and reports what it is.
    ///
    /// This is the single explicit restoration transition. It succeeds at most
    /// once per generation, never sends two create commands on one connection,
    /// and never runs from a background task: a caller that wants the transport
    /// back after a failure calls this again, so recovery policy stays outside
    /// this crate.
    pub async fn establish_primary(
        &self,
        cancellation: &Cancellation,
    ) -> Result<SamNegotiated, TransportError> {
        self.begin()?;
        let result = self.establish_primary_inner(cancellation).await;
        self.end();
        result
    }

    async fn establish_primary_inner(
        &self,
        cancellation: &Cancellation,
    ) -> Result<SamNegotiated, TransportError> {
        if let Some(live) = self.lock()?.live.as_ref() {
            return Ok(live.negotiated);
        }
        let mut last: Option<TransportError> = None;
        for style in SamPrimaryStyle::OFFER_ORDER {
            // Every attempt gets its own control connection: a service that
            // rejected the first spelling created no session, and a second
            // command on the same connection could leave two owners behind.
            let mut stream = self
                .open_connection(cancellation, self.timeouts.open)
                .await?;
            let version = self
                .negotiate_hello(&mut stream, cancellation, self.timeouts.handshake)
                .await?;
            if version < SamVersion::new(3, 3) {
                return Err(SamError::SharedSessionUnsupported.into());
            }
            let command = encode_session_create_primary(
                self.session_id(),
                self.session_destination(),
                style,
            )?;
            let reply = match exchange(
                &mut stream,
                &command,
                self.limits,
                cancellation,
                self.timeouts.handshake,
                None,
            )
            .await
            {
                Ok(reply) => reply,
                Err(error) => {
                    last = Some(error);
                    continue;
                }
            };
            match reply.result() {
                Some(SamResult::Ok) => {}
                // A style the service does not know, or a session name it
                // rejects, is the only reason to offer the other spelling.
                Some(SamResult::I2pError | SamResult::InvalidKey) => {
                    last = Some(SamError::Router(SamResult::I2pError).into());
                    continue;
                }
                Some(other) => return Err(SamError::Router(other).into()),
                None => return Err(TransportError::Protocol),
            };
            let local = self
                .resolve_local_identity(&mut stream, &reply, cancellation)
                .await?;
            let negotiated = SamNegotiated {
                version,
                primary_style: style,
            };
            self.adopt_primary(stream, negotiated, local)?;
            return Ok(negotiated);
        }
        Err(last.unwrap_or(SamError::SharedSessionUnsupported.into()))
    }

    /// Resolves the local Destination the router actually bound this session to.
    ///
    /// A `SESSION CREATE` reply may carry the Destination directly; a transient
    /// session may instead carry private key material in the same option, which
    /// is not a Destination and must never be cached as one. `NAMING LOOKUP
    /// NAME=ME` is the portable confirmation path and is always used when the
    /// reply does not carry a structurally valid Destination.
    async fn resolve_local_identity(
        &self,
        stream: &mut SamRawStream,
        reply: &SamReply,
        cancellation: &Cancellation,
    ) -> Result<SamLocalIdentity, TransportError> {
        if let Some(value) = reply.option("DESTINATION")
            && let Ok(bytes) = decode_base64(value.as_bytes(), MAX_DESTINATION_BYTES)
            && is_destination_structure(&bytes)
            && let Ok(destination) = identity::Destination::from_bytes(bytes)
        {
            return Ok(SamLocalIdentity { destination });
        }
        let command = encode_naming_lookup("ME")?;
        let naming = exchange(
            stream,
            &command,
            self.limits,
            cancellation,
            self.timeouts.lookup,
            Some("VALUE"),
        )
        .await?;
        naming.require_ok()?;
        Ok(SamLocalIdentity {
            destination: naming.destination("VALUE")?,
        })
    }

    /// Installs a freshly created primary and starts its control supervisor.
    fn adopt_primary(
        &self,
        stream: SamRawStream,
        negotiated: SamNegotiated,
        local: SamLocalIdentity,
    ) -> Result<(), TransportError> {
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        let (commands, receiver) = mpsc::channel(PRIMARY_QUEUE_DEPTH);
        // A weak handle keeps the primary's ownership one-directional: the
        // client owns the session, and the session's supervisor can never keep
        // the client's state alive.
        let supervisor = tokio::spawn(supervise_primary(
            stream,
            receiver,
            self.limits,
            Arc::downgrade(&self.state),
            generation,
        ));
        let mut state = self.lock()?;
        if let Some(previous) = state.live.replace(LivePrimary {
            generation,
            negotiated,
            local,
            commands,
            _supervisor: supervisor,
            channels: 0,
        }) {
            drop(previous);
        }
        state.available = true;
        Ok(())
    }

    /// Adds one child channel to the live primary session.
    ///
    /// `SESSION ADD` is issued on the primary's own control connection, because
    /// that is the only connection a bridge accepts it on.
    pub async fn add_channel(
        &self,
        kind: SamChannelKind,
        config: SamChannelConfig,
        cancellation: &Cancellation,
    ) -> Result<SamChannel, TransportError> {
        self.begin()?;
        let result = self.add_channel_inner(kind, config, cancellation).await;
        self.end();
        result
    }

    async fn add_channel_inner(
        &self,
        kind: SamChannelKind,
        config: SamChannelConfig,
        cancellation: &Cancellation,
    ) -> Result<SamChannel, TransportError> {
        let generation = self.require_live_generation()?;
        let id = self.next_child_id(kind);
        let command = encode_session_add(kind, &id, config)?;
        self.control(
            generation,
            &command,
            cancellation,
            self.timeouts.child,
            None,
        )
        .await?
        .require_ok()?;
        let mut state = self.lock()?;
        let Some(live) = state.live.as_mut() else {
            return Err(SamError::StaleGeneration.into());
        };
        if live.generation != generation {
            return Err(SamError::StaleGeneration.into());
        }
        live.channels += 1;
        Ok(SamChannel {
            id,
            kind,
            generation,
        })
    }

    /// Removes one child channel from the live primary session.
    ///
    /// Removal is issued on the control connection and is bounded: a channel
    /// whose generation has already been replaced is reported stale rather than
    /// removed from the wrong identity.
    pub async fn remove_channel(
        &self,
        channel: &SamChannel,
        cancellation: &Cancellation,
    ) -> Result<(), TransportError> {
        self.begin()?;
        let result = self.remove_channel_inner(channel, cancellation).await;
        self.end();
        result
    }

    async fn remove_channel_inner(
        &self,
        channel: &SamChannel,
        cancellation: &Cancellation,
    ) -> Result<(), TransportError> {
        let generation = self.require_channel_generation(channel)?;
        let command = encode_session_remove(channel.id())?;
        let reply = self
            .control(
                generation,
                &command,
                cancellation,
                self.timeouts.child,
                None,
            )
            .await?;
        reply.require_ok()?;
        let mut state = self.lock()?;
        if let Some(live) = state.live.as_mut()
            && live.generation == channel.generation
        {
            live.channels = live.channels.saturating_sub(1);
        }
        Ok(())
    }

    /// Opens an outbound peer stream on the live primary's STREAM channel.
    ///
    /// The connection issues `HELLO` and `STREAM CONNECT` only: it never issues
    /// a `SESSION CREATE`, because the identity already exists.
    pub async fn stream_connect(
        &self,
        channel: &SamChannel,
        destination: &identity::Destination,
        port: u16,
        cancellation: &Cancellation,
    ) -> Result<I2pStream, TransportError> {
        let generation = self.require_channel_generation(channel)?;
        self.begin()?;
        let result = async {
            let command = encode_stream_connect(channel.id(), destination.as_bytes(), port)?;
            let mut stream = self
                .open_connection(cancellation, self.timeouts.open)
                .await?;
            self.negotiate_hello(&mut stream, cancellation, self.timeouts.handshake)
                .await?;
            exchange(
                &mut stream,
                &command,
                self.limits,
                cancellation,
                self.timeouts.connect,
                None,
            )
            .await?
            .require_ok()?;
            self.assert_generation(generation)?;
            Ok(stream)
        }
        .await;
        self.end();
        result
    }

    /// Accepts one inbound peer stream on the live primary's STREAM channel.
    ///
    /// The peer that connected is reported on the line after the status line,
    /// carrying the Destination on its own with no option key, and everything
    /// after it is already raw peer data. Reading it as a `KEY=VALUE` line
    /// would consume the peer's first bytes instead.
    pub async fn stream_accept(
        &self,
        channel: &SamChannel,
        cancellation: &Cancellation,
    ) -> Result<(identity::Destination, I2pStream), TransportError> {
        let generation = self.require_channel_generation(channel)?;
        self.begin()?;
        let result = async {
            let command = encode_stream_accept(channel.id())?;
            let mut stream = self
                .open_connection(cancellation, self.timeouts.open)
                .await?;
            self.negotiate_hello(&mut stream, cancellation, self.timeouts.handshake)
                .await?;
            exchange(
                &mut stream,
                &command,
                self.limits,
                cancellation,
                self.timeouts.accept,
                None,
            )
            .await?
            .require_ok()?;
            let peer = self
                .read_bare_destination(&mut stream, cancellation)
                .await?;
            self.assert_generation(generation)?;
            Ok((peer, stream))
        }
        .await;
        self.end();
        result
    }

    /// Sends one bounded datagram over the primary's shared Destination.
    ///
    /// The datagram is framed on the bridge socket; this client never asks the
    /// router for a host datagram socket to do it.
    pub async fn datagram_send(
        &self,
        channel: &SamChannel,
        destination: &identity::Destination,
        config: SamChannelConfig,
        payload: &[u8],
        cancellation: &Cancellation,
    ) -> Result<(), TransportError> {
        self.send_datagram(
            channel,
            SamChannelKind::RepliableDatagram,
            destination,
            config,
            payload,
            cancellation,
        )
        .await
    }

    /// Sends one bounded raw datagram over the primary's shared Destination.
    pub async fn raw_send(
        &self,
        channel: &SamChannel,
        destination: &identity::Destination,
        config: SamChannelConfig,
        payload: &[u8],
        cancellation: &Cancellation,
    ) -> Result<(), TransportError> {
        self.send_datagram(
            channel,
            SamChannelKind::RawDatagram,
            destination,
            config,
            payload,
            cancellation,
        )
        .await
    }

    async fn send_datagram(
        &self,
        channel: &SamChannel,
        kind: SamChannelKind,
        destination: &identity::Destination,
        config: SamChannelConfig,
        payload: &[u8],
        cancellation: &Cancellation,
    ) -> Result<(), TransportError> {
        let generation = self.require_channel_generation(channel)?;
        if payload.len() > self.limits.max_datagram_bytes {
            return Err(SamError::Limit.into());
        }
        self.begin()?;
        let result = async {
            let command = match kind {
                SamChannelKind::RepliableDatagram => {
                    encode_datagram_send(channel.id(), destination.as_bytes(), config, payload)?
                }
                _ => encode_raw_send(channel.id(), destination.as_bytes(), config, payload)?,
            };
            let mut stream = self
                .open_connection(cancellation, self.timeouts.open)
                .await?;
            self.negotiate_hello(&mut stream, cancellation, self.timeouts.handshake)
                .await?;
            exchange(
                &mut stream,
                &command,
                self.limits,
                cancellation,
                self.timeouts.datagram,
                None,
            )
            .await?
            .require_ok()?;
            self.assert_generation(generation)?;
            Ok(())
        }
        .await;
        self.end();
        result
    }

    /// Receives one bounded datagram for the primary's shared Destination.
    ///
    /// Cancellation and the receive deadline are the only backpressure this
    /// primitive applies; a caller that must not block a task waits on it with a
    /// [`Cancellation`] it owns.
    pub async fn datagram_receive(
        &self,
        channel: &SamChannel,
        cancellation: &Cancellation,
    ) -> Result<SamDatagram, TransportError> {
        let datagram = self
            .receive_datagram(channel, SamChannelKind::RepliableDatagram, cancellation)
            .await?;
        Ok(SamDatagram {
            sender: datagram.sender,
            payload: datagram.payload,
            from_port: datagram.from_port,
            to_port: datagram.to_port,
        })
    }

    /// Receives one bounded raw datagram for the primary's shared Destination.
    pub async fn raw_receive(
        &self,
        channel: &SamChannel,
        cancellation: &Cancellation,
    ) -> Result<SamRawDatagram, TransportError> {
        let datagram = self
            .receive_datagram(channel, SamChannelKind::RawDatagram, cancellation)
            .await?;
        Ok(SamRawDatagram {
            payload: datagram.payload,
            protocol: datagram.protocol,
            from_port: datagram.from_port,
            to_port: datagram.to_port,
            sender: datagram.sender,
        })
    }

    async fn receive_datagram(
        &self,
        channel: &SamChannel,
        kind: SamChannelKind,
        cancellation: &Cancellation,
    ) -> Result<TransportDatagram, TransportError> {
        let generation = self.require_channel_generation(channel)?;
        self.begin()?;
        let result = async {
            let command = match kind {
                SamChannelKind::RepliableDatagram => encode_datagram_receive(channel.id())?,
                _ => encode_raw_receive(channel.id())?,
            };
            let mut stream = self
                .open_connection(cancellation, self.timeouts.open)
                .await?;
            self.negotiate_hello(&mut stream, cancellation, self.timeouts.handshake)
                .await?;
            let reply = exchange(
                &mut stream,
                &command,
                self.limits,
                cancellation,
                self.timeouts.datagram,
                Some("SIZE"),
            )
            .await?;
            let size: u32 = reply.number("SIZE")?;
            if size as usize > self.limits.max_datagram_bytes {
                return Err(SamError::Limit.into());
            }
            let payload = read_exact(
                &mut stream,
                size as usize,
                self.limits,
                cancellation,
                self.timeouts.datagram,
            )
            .await?;
            let (protocol, from_port, to_port, sender) = match kind {
                SamChannelKind::RepliableDatagram => (
                    I2P_PROTOCOL_REPLIABLE,
                    reply.number_or("FROM_PORT", 0u16)?,
                    reply.number_or("TO_PORT", 0u16)?,
                    reply
                        .option("DESTINATION")
                        .and_then(|value| decode_destination(value).ok()),
                ),
                _ => (
                    reply.number("PROTOCOL")?,
                    reply.number_or("FROM_PORT", 0u16)?,
                    reply.number_or("TO_PORT", 0u16)?,
                    reply
                        .option("DESTINATION")
                        .and_then(|value| decode_destination(value).ok()),
                ),
            };
            self.assert_generation(generation)?;
            Ok(TransportDatagram {
                payload,
                protocol,
                from_port,
                to_port,
                sender,
            })
        }
        .await;
        self.end();
        result
    }

    /// Resolves a naming entry to a verified Destination.
    ///
    /// The reply carries the Destination under `VALUE`, not under a
    /// `DESTINATION` key: `DESTINATION` names the key a `SESSION CREATE`
    /// command carries, and reusing it here would leave this waiting on a
    /// second line a service never sends.
    pub async fn nam_lookup(
        &self,
        name: &str,
        cancellation: &Cancellation,
    ) -> Result<identity::Destination, TransportError> {
        self.begin()?;
        let result = async {
            let command = encode_naming_lookup(name)?;
            let mut stream = self
                .open_connection(cancellation, self.timeouts.open)
                .await?;
            self.negotiate_hello(&mut stream, cancellation, self.timeouts.handshake)
                .await?;
            let reply = exchange(
                &mut stream,
                &command,
                self.limits,
                cancellation,
                self.timeouts.lookup,
                Some("VALUE"),
            )
            .await?;
            reply.require_ok()?;
            reply.destination("VALUE").map_err(Into::into)
        }
        .await;
        self.end();
        result
    }

    /// The STREAM channel of the live primary, created on first use.
    ///
    /// This is the only implicit child creation in the client, it happens at
    /// most once per generation, and it never issues `SESSION CREATE`.
    pub async fn stream_channel(
        &self,
        cancellation: &Cancellation,
    ) -> Result<SamChannel, TransportError> {
        let generation = self.require_live_generation()?;
        if let Some(channel) = self
            .channel_cache
            .lock()
            .ok()
            .and_then(|cache| cache.get(&generation).cloned())
        {
            return Ok(channel);
        }
        let channel = self
            .add_channel(
                SamChannelKind::Stream,
                SamChannelConfig::STREAM,
                cancellation,
            )
            .await?;
        if let Ok(mut cache) = self.channel_cache.lock() {
            // A new generation is a new identity, so a cached channel from an
            // older one is dropped rather than reused.
            cache.retain(|_, cached| cached.id() != channel.id());
            cache.insert(generation, channel.clone());
        }
        Ok(channel)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, PrimaryState>, TransportError> {
        self.state
            .lock()
            .map_err(|_| TransportError::Session("sam session state is poisoned".to_owned()))
    }

    fn require_live_generation(&self) -> Result<u64, TransportError> {
        self.lock()?
            .live
            .as_ref()
            .map(|live| live.generation)
            .ok_or(TransportError::IdentityNotReady)
    }

    /// A child channel is usable only while the live primary is the one it was
    /// attached to. Anything else is stale, including the case where its primary
    /// is gone entirely: it can never attach to another identity.
    fn require_channel_generation(&self, channel: &SamChannel) -> Result<u64, TransportError> {
        let live = self
            .lock()?
            .live
            .as_ref()
            .map(|live| live.generation)
            .ok_or(SamError::StaleGeneration)?;
        if live != channel.generation {
            return Err(SamError::StaleGeneration.into());
        }
        Ok(live)
    }

    fn assert_generation(&self, generation: u64) -> Result<(), TransportError> {
        if self.require_live_generation()? != generation {
            return Err(SamError::StaleGeneration.into());
        }
        Ok(())
    }

    /// A per-kind counter, so a client's identifiers stay readable and stable
    /// (`…-stream-1`, `…-datagram-1`, `…-raw-1`) across a session's lifetime.
    fn next_child_id(&self, kind: SamChannelKind) -> String {
        let slot = match kind {
            SamChannelKind::Stream => 0,
            SamChannelKind::RepliableDatagram => 1,
            SamChannelKind::RawDatagram => 2,
        };
        let counter = self.child_counters[slot].fetch_add(1, Ordering::SeqCst) + 1;
        format!("{}-{}-{counter}", self.session_id(), kind.label())
    }

    /// Sends one command on the primary's own control connection.
    async fn control(
        &self,
        generation: u64,
        octets: &[u8],
        cancellation: &Cancellation,
        timeout: Duration,
        want: Option<&'static str>,
    ) -> Result<SamReply, TransportError> {
        let commands = {
            let state = self.lock()?;
            let live = state
                .live
                .as_ref()
                .ok_or(TransportError::IdentityNotReady)?;
            if live.generation != generation {
                return Err(SamError::StaleGeneration.into());
            }
            live.commands.clone()
        };
        let (reply, answer) = oneshot::channel();
        // A full queue is explicit backpressure rather than unbounded buffering:
        // the primary connection is the one resource that cannot be duplicated.
        commands
            .try_send(PrimaryCommand {
                octets: octets.to_vec(),
                want,
                reply,
            })
            .map_err(|_| SamError::Backpressure)?;
        // The supervisor owns the control connection, so its reply and its
        // terminal failure both arrive here as the answer value.
        let answer = self.bounded(answer, timeout, cancellation).await?;
        match answer {
            Ok(reply) => reply,
            // The supervisor ended without answering: the control connection is
            // gone, so every child of this generation is stale.
            Err(_) => Err(TransportError::Stale(
                "sam primary control connection ended",
            )),
        }
    }

    fn begin(&self) -> Result<(), TransportError> {
        if self.shutdown.is_cancelled() {
            return Err(TransportError::Cancelled);
        }
        let mut state = self.lock()?;
        if !state.available {
            return Err(TransportError::Session(
                "sam session is unavailable".to_owned(),
            ));
        }
        state.in_flight += 1;
        Ok(())
    }

    fn end(&self) {
        if let Ok(mut state) = self.lock() {
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }

    async fn bounded<T, FUT: Future<Output = T>>(
        &self,
        future: FUT,
        timeout: Duration,
        cancellation: &Cancellation,
    ) -> Result<T, TransportError> {
        tokio::pin!(future);
        let raced = async {
            loop {
                if cancellation.is_cancelled() || self.shutdown.is_cancelled() {
                    return Err(TransportError::Cancelled);
                }
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep(CANCELLATION_POLL) => {}
                    output = &mut future => return Ok(output),
                }
            }
        };
        match tokio::time::timeout(timeout, raced).await {
            Ok(output) => output,
            Err(_) => Err(TransportError::Timeout),
        }
    }

    async fn open_connection(
        &self,
        cancellation: &Cancellation,
        open_timeout: Duration,
    ) -> Result<SamRawStream, TransportError> {
        self.bounded(
            self.factory.open_sam_connection(),
            open_timeout,
            cancellation,
        )
        .await?
    }

    /// Negotiates `HELLO` only.
    ///
    /// Attaching a connection to an existing session must not create one, so no
    /// `SESSION CREATE` is sent here. This is the separation C003 exists to
    /// make: attachment negotiation and session creation are different steps.
    async fn negotiate_hello(
        &self,
        stream: &mut SamRawStream,
        cancellation: &Cancellation,
        timeout: Duration,
    ) -> Result<SamVersion, TransportError> {
        let command = encode_hello(self.min_version, self.max_version)?;
        let reply = exchange(stream, &command, self.limits, cancellation, timeout, None).await?;
        if reply.kind() != SamReplyKind::Hello {
            return Err(TransportError::Protocol);
        }
        reply.require_ok()?;
        let version = SamVersion::parse(
            reply
                .option("VERSION")
                .ok_or(SamError::Missing("VERSION"))?,
        )
        .map_err(TransportError::from)?;
        if !version.within(self.min_version, self.max_version) {
            return Err(TransportError::Session(format!(
                "sam advertised unsupported version {version}"
            )));
        }
        Ok(version)
    }

    /// Reads the bare Destination line a successful accept is followed by.
    async fn read_bare_destination(
        &self,
        stream: &mut SamRawStream,
        cancellation: &Cancellation,
    ) -> Result<identity::Destination, TransportError> {
        let limits = self.limits;
        self.bounded(
            async {
                let mut budget = limits.max_reply_bytes;
                let raw = read_line(stream, &limits, &mut budget).await?;
                let text = std::str::from_utf8(trim_ending(&raw))
                    .map_err(|_| SamError::Protocol("SAM destination line is not UTF-8"))?;
                if text.len() > limits.max_value_bytes {
                    return Err(TransportError::from(SamError::Limit));
                }
                decode_destination(text).map_err(Into::into)
            },
            self.timeouts.accept,
            cancellation,
        )
        .await?
    }
}

/// A received datagram before it is split into its public forms.
struct TransportDatagram {
    payload: Vec<u8>,
    protocol: u8,
    from_port: u16,
    to_port: u16,
    sender: Option<identity::Destination>,
}

/// Runs the primary control connection for its whole lifetime.
///
/// The connection is first-class owned state: it is created once per
/// generation, every `SESSION ADD`/`SESSION REMOVE` runs on it, and only the
/// supervisor reads from it. End-of-file, an I/O failure, or a reply this
/// client cannot parse clears the primary, which makes every child of that
/// generation stale.
async fn supervise_primary(
    mut stream: SamRawStream,
    mut commands: mpsc::Receiver<PrimaryCommand>,
    limits: SamLimits,
    state: std::sync::Weak<Mutex<PrimaryState>>,
    generation: u64,
) {
    while let Some(command) = commands.recv().await {
        let result = exchange_free(
            &mut stream,
            &command.octets,
            limits,
            command.want,
            PRIMARY_COMMAND_TIMEOUT,
        )
        .await;
        let healthy = !matches!(
            result,
            Err(TransportError::Io(_))
                | Err(TransportError::Protocol)
                | Err(TransportError::Timeout)
                | Err(TransportError::Stale(_))
        );
        let _ = command.reply.send(result);
        if !healthy {
            break;
        }
    }
    if let Some(state) = state.upgrade()
        && let Ok(mut state) = state.lock()
        && state
            .live
            .as_ref()
            .is_some_and(|live| live.generation == generation)
    {
        // Losing the primary is a transport-wide session event: children of this
        // generation are stale, and only an explicit re-establishment may mint a
        // replacement identity.
        state.live = None;
    }
}

/// Bound on one command issued on the primary control connection.
const PRIMARY_COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

/// Writes one command and reads its reply on an owned connection.
async fn exchange(
    stream: &mut SamRawStream,
    command: &[u8],
    limits: SamLimits,
    cancellation: &Cancellation,
    timeout: Duration,
    want: Option<&'static str>,
) -> Result<SamReply, TransportError> {
    let deadline = tokio::time::Instant::now() + timeout;
    tokio::select! {
        biased;
        output = exchange_free(stream, command, limits, want, timeout) => output,
        () = wait_cancelled(cancellation, deadline) => Err(TransportError::Cancelled),
    }
}

/// Reads one command's reply with no external cancellation source.
async fn exchange_free(
    stream: &mut SamRawStream,
    command: &[u8],
    limits: SamLimits,
    want: Option<&'static str>,
    timeout: Duration,
) -> Result<SamReply, TransportError> {
    let work = async {
        stream.write_all(command).await?;
        stream.flush().await?;
        let mut budget = limits.max_reply_bytes;
        let raw_line = read_line(stream, &limits, &mut budget).await?;
        let mut reply = parse_status_line(trim_ending(&raw_line), &limits)?;
        // A following `KEY=VALUE` line is read only when the status line omitted
        // a value this command needs, so a connection that is already raw peer
        // data never blocks here.
        if let Some(key) = want
            && reply.option(key).is_none()
        {
            match reply.result() {
                Some(SamResult::Ok) => {
                    let raw = read_line(stream, &limits, &mut budget).await?;
                    let text = std::str::from_utf8(trim_ending(&raw))
                        .map_err(|_| SamError::Protocol("SAM reply line is not UTF-8"))?;
                    apply_token(&mut reply, text, &limits)?;
                    if reply.option(key).is_none() {
                        return Err(TransportError::from(SamError::Missing(key)));
                    }
                }
                Some(other) => return Err(TransportError::from(SamError::Router(other))),
                None => {
                    return Err(TransportError::from(SamError::Protocol(
                        "SAM reply is missing RESULT",
                    )));
                }
            }
        }
        if reply.result.is_none() && !reply.delivery {
            return Err(TransportError::from(SamError::Protocol(
                "SAM reply is missing RESULT",
            )));
        }
        Ok(reply)
    };
    match tokio::time::timeout(timeout, work).await {
        Ok(result) => result,
        Err(_) => Err(TransportError::Timeout),
    }
}

/// Polls one cancellation token until the deadline, then stops.
async fn wait_cancelled(cancellation: &Cancellation, deadline: tokio::time::Instant) {
    loop {
        if cancellation.is_cancelled() || tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(CANCELLATION_POLL).await;
    }
}

/// Reads exactly `size` bytes of a received datagram, bounded by the limits.
async fn read_exact(
    stream: &mut SamRawStream,
    size: usize,
    limits: SamLimits,
    cancellation: &Cancellation,
    timeout: Duration,
) -> Result<Vec<u8>, TransportError> {
    if size > limits.max_datagram_bytes {
        return Err(SamError::Limit.into());
    }
    let work = async {
        let mut payload = vec![0u8; size];
        stream.read_exact(&mut payload).await?;
        Ok(payload)
    };
    tokio::select! {
        biased;
        output = tokio::time::timeout(timeout, work) => match output {
            Ok(result) => result,
            Err(_) => Err(TransportError::Timeout),
        },
        () = wait_cancelled(cancellation, tokio::time::Instant::now() + timeout) => Err(TransportError::Cancelled),
    }
}

impl<F: SamConnectionFactory> Drop for SamClient<F> {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[async_trait]
impl<F: SamConnectionFactory> I2pSession for SamClient<F> {
    /// The router-confirmed local Destination hash, or a typed not-ready error.
    fn local_peer_hash(&self) -> Result<[u8; 32], TransportError> {
        self.local_destination_hash()
    }

    async fn lookup(&self, name: &str) -> Result<identity::Destination, TransportError> {
        self.nam_lookup(name, &Cancellation::default()).await
    }

    async fn connect(
        &self,
        destination: &identity::Destination,
        port: u16,
    ) -> Result<I2pStream, TransportError> {
        let channel = self.stream_channel(&Cancellation::default()).await?;
        self.stream_connect(&channel, destination, port, &Cancellation::default())
            .await
    }

    async fn accept(&self) -> Result<(identity::Destination, I2pStream), TransportError> {
        let channel = self.stream_channel(&Cancellation::default()).await?;
        self.stream_accept(&channel, &Cancellation::default()).await
    }
}
