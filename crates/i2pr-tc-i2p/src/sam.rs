//! Application-owned SAM v3.1 protocol client over raw SAM connections.
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
//! SAM's own model is multi-connection: one logical torrent identity owns a
//! bounded application session identifier that is shared by several raw
//! protocol connections. Every raw connection is independently negotiated with
//! `HELLO VERSION`, joins the session with `SESSION CREATE`, and carries
//! exactly one further command (`NAMING LOOKUP`, `STREAM CONNECT`, or
//! `STREAM ACCEPT`) before its remaining bytes are raw peer data. Forwarding
//! sockets is deliberately not implemented: it is unavailable in the managed
//! profile and is not required by the torrent transport.
#![forbid(unsafe_code)]

use crate::{I2pSession, I2pStream, TransportError, identity};
use async_trait::async_trait;
use i2pr_tc_storage::Cancellation;
#[cfg(test)]
use sha2::Digest;
#[cfg(test)]
use std::sync::Arc;
use std::{collections::VecDeque, future::Future, io, sync::Mutex, time::Duration};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Smallest Destination accepted by [`crate::identity::Destination`].
const MIN_DESTINATION_BYTES: usize = 387;
/// Largest Destination accepted by [`crate::identity::Destination`].
const MAX_DESTINATION_BYTES: usize = 8192;
/// Bound on a command token (session identifier or naming lookup name).
const MAX_COMMAND_TOKEN_BYTES: usize = 512;
/// Cancellation poll granularity for cancellable operations.
const CANCELLATION_POLL: Duration = Duration::from_millis(20);
/// Wait granularity while draining in-flight work during [`SamClient::close`].
const CLOSE_POLL: Duration = Duration::from_millis(10);

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
}

impl Default for SamLimits {
    fn default() -> Self {
        Self {
            max_line_bytes: 16 * 1024,
            max_reply_bytes: 32 * 1024,
            max_token_bytes: 64,
            max_value_bytes: 12 * 1024,
            max_options: 8,
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
        }
    }

    /// Rejects a bound set that cannot hold one option value on one line.
    pub fn validate(self) -> Result<(), SamError> {
        let smallest = self.max_token_bytes.checked_add(self.max_value_bytes);
        if self.max_token_bytes < 8
            || self.max_value_bytes < 8
            || self.max_options < 1
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
}

impl From<SamError> for TransportError {
    fn from(error: SamError) -> Self {
        match error {
            SamError::Io(inner) => TransportError::Io(inner),
            SamError::Timeout => TransportError::Timeout,
            SamError::Cancelled => TransportError::Cancelled,
            SamError::Router(result) => {
                TransportError::Session(format!("sam reply reported {result:?}"))
            }
            SamError::Unavailable(reason) => TransportError::Session(reason.to_owned()),
            SamError::Configuration
            | SamError::Limit
            | SamError::Protocol(_)
            | SamError::Missing(_) => TransportError::Protocol,
        }
    }
}

/// The SAM v3 result vocabulary this client recognises.
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
    Timeout,
    CantReachPeer,
    ConnectionRefused,
    NotImplemented,
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
            "DUPLICATED_DESTINATION" => Self::DuplicatedDestination,
            "INVALID_ID" => Self::InvalidId,
            "INVALID_NAME" => Self::InvalidName,
            "TIMEOUT" => Self::Timeout,
            "CANT_REACH_PEER" => Self::CantReachPeer,
            "CONNECTION_REFUSED" => Self::ConnectionRefused,
            "NOT_IMPLEMENTED" => Self::NotImplemented,
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
}

/// A parsed SAM reply block: one status line plus its `KEY=VALUE` lines.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SamReply {
    kind: SamReplyKind,
    result: Option<SamResult>,
    options: Vec<(String, String)>,
}

impl SamReply {
    pub fn kind(&self) -> SamReplyKind {
        self.kind
    }

    pub fn result(&self) -> Option<SamResult> {
        self.result
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

    /// Decodes a base64 Destination option into a verified Destination.
    pub fn destination(&self, key: &'static str) -> Result<identity::Destination, SamError> {
        let value = self.option(key).ok_or(SamError::Missing(key))?;
        // A destination that is merely too large stays a limit violation; a
        // destination that is not base64 at all is a protocol violation.
        let bytes = match decode_base64(value.as_bytes(), MAX_DESTINATION_BYTES) {
            Ok(bytes) => bytes,
            Err(SamError::Limit) => return Err(SamError::Limit),
            Err(_) => return Err(SamError::Protocol("SAM destination is not decodable")),
        };
        if !(MIN_DESTINATION_BYTES..=MAX_DESTINATION_BYTES).contains(&bytes.len()) {
            return Err(SamError::Protocol("SAM destination length out of range"));
        }
        identity::Destination::from_bytes(bytes)
            .map_err(|_| SamError::Protocol("SAM destination is not addressable"))
    }
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
        Ok(Self {
            major: parse_decimal(major.as_bytes())?,
            minor: parse_decimal(minor.as_bytes())?,
        })
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

fn parse_decimal(digits: &[u8]) -> Result<u32, SamError> {
    if digits.is_empty() || digits.len() > 10 {
        return Err(SamError::Protocol("non-decimal SAM number"));
    }
    let mut value = 0u32;
    for digit in digits {
        if !digit.is_ascii_digit() {
            return Err(SamError::Protocol("non-decimal SAM number"));
        }
        value = value
            .checked_mul(10)
            .and_then(|scaled| scaled.checked_add(u32::from(digit - b'0')))
            .ok_or(SamError::Protocol("overflowing SAM number"))?;
    }
    Ok(value)
}

/// Encodes `bytes` as standard base64 with padding.
pub fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
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

/// Decodes standard base64, rejecting non-alphabet bytes, bad padding, and
/// inputs that could exceed `max_bytes`.
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
        b'+' => 62,
        b'/' => 63,
        _ => return None,
    })
}

/// Exact `HELLO VERSION` command octets.
pub fn encode_hello(min: SamVersion, max: SamVersion) -> Result<Vec<u8>, SamError> {
    Ok(format!("HELLO VERSION MIN={min} MAX={max}\n").into_bytes())
}

/// Exact `SESSION CREATE STYLE=STREAM` command octets.
pub fn encode_session_create(session_id: &str) -> Result<Vec<u8>, SamError> {
    validate_token(session_id)?;
    Ok(format!("SESSION CREATE STYLE=STREAM ID={session_id}\n").into_bytes())
}

/// Exact `NAMING LOOKUP` command octets.
pub fn encode_naming_lookup(name: &str) -> Result<Vec<u8>, SamError> {
    validate_token(name)?;
    Ok(format!("NAMING LOOKUP NAME={name}\n").into_bytes())
}

/// Exact `STREAM CONNECT` command octets, including an explicit port.
pub fn encode_stream_connect(
    session_id: &str,
    destination: &[u8],
    port: u16,
) -> Result<Vec<u8>, SamError> {
    validate_token(session_id)?;
    if destination.len() > MAX_DESTINATION_BYTES {
        return Err(SamError::Limit);
    }
    Ok(format!(
        "STREAM CONNECT ID={session_id} DESTINATION={} PORT={port} SILENT=false\n",
        encode_base64(destination)
    )
    .into_bytes())
}

/// Exact `STREAM ACCEPT` command octets.
pub fn encode_stream_accept(session_id: &str) -> Result<Vec<u8>, SamError> {
    validate_token(session_id)?;
    Ok(format!("STREAM ACCEPT ID={session_id} SILENT=false\n").into_bytes())
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
    if reply.result.is_none() {
        return Err(SamError::Protocol("SAM reply is missing RESULT"));
    }
    Ok(reply)
}

fn parse_status_line(line: &[u8], limits: &SamLimits) -> Result<SamReply, SamError> {
    if line.len() > limits.max_line_bytes {
        return Err(SamError::Limit);
    }
    let text = std::str::from_utf8(line)
        .map_err(|_| SamError::Protocol("SAM status line is not UTF-8"))?;
    let mut tokens = text.split(' ');
    let command = tokens.next().unwrap_or_default();
    let kind = match command {
        "HELLO" => SamReplyKind::Hello,
        "NAMING" => SamReplyKind::Naming,
        "SESSION" => SamReplyKind::Session,
        "STREAM" => SamReplyKind::Stream,
        _ => return Err(SamError::Protocol("unknown SAM reply kind")),
    };
    let keyword = tokens.next().unwrap_or_default();
    let expected = match kind {
        SamReplyKind::Hello | SamReplyKind::Naming => "REPLY",
        SamReplyKind::Session | SamReplyKind::Stream => "STATUS",
    };
    if keyword != expected {
        return Err(SamError::Protocol("unknown SAM reply kind"));
    }
    let mut reply = SamReply {
        kind,
        result: None,
        options: Vec::new(),
    };
    for token in tokens {
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

/// The application torrent identity and its single bounded SAM session id.
///
/// The private destination is an injected value: this type never reads a
/// filesystem secret, environment variable, or any other ambient authority.
/// Deciding whether and where destination key material is persisted belongs to
/// the managed runtime composition layer. When no destination is injected the
/// client can still serve inbound accepts, but its local hash is a documented
/// placeholder rather than a connectable peer identity.
#[derive(Clone, Debug)]
pub struct SamIdentity {
    destination: Option<identity::Destination>,
    session_id: String,
    hash: [u8; 32],
}

impl SamIdentity {
    /// Builds an identity from an optional base64 private destination string.
    pub fn new(private_destination: Option<&str>, session_id: &str) -> Result<Self, SamError> {
        validate_token(session_id)?;
        let destination = match private_destination {
            None => None,
            Some(encoded) => {
                let bytes = decode_base64(encoded.as_bytes(), MAX_DESTINATION_BYTES)?;
                if !(MIN_DESTINATION_BYTES..=MAX_DESTINATION_BYTES).contains(&bytes.len()) {
                    return Err(SamError::Protocol("SAM destination length out of range"));
                }
                Some(
                    identity::Destination::from_bytes(bytes)
                        .map_err(|_| SamError::Protocol("SAM destination is not addressable"))?,
                )
            }
        };
        let hash = destination
            .as_ref()
            .map(identity::Destination::hash)
            .unwrap_or_else(|| session_identity_hash(session_id));
        Ok(Self {
            destination,
            session_id: session_id.to_owned(),
            hash,
        })
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn destination(&self) -> Option<&identity::Destination> {
        self.destination.as_ref()
    }

    /// The eagerly computed local peer hash.
    pub fn hash(&self) -> [u8; 32] {
        self.hash
    }

    /// Whether a real connectable Destination was injected.
    pub fn has_destination(&self) -> bool {
        self.destination.is_some()
    }
}

fn session_identity_hash(session_id: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"unresolved-sam-session-identity\0");
    digest.update(session_id.as_bytes());
    digest.finalize().into()
}

#[derive(Debug, Default)]
struct SessionState {
    created: bool,
    available: bool,
    negotiated: Option<SamVersion>,
    in_flight: usize,
}

/// Owner of one application torrent SAM session.
///
/// The client owns exactly one bounded session identifier and the lifecycle of
/// every raw connection that joins it. Ordinary connection loss does not
/// destroy the torrent identity: each operation acquires its own fresh
/// connection. A failure of the shared session marks the transport unavailable
/// so dependent stream operations fail fast instead of retrying, and
/// [`SamClient::close`] plus `Drop` cancel in-flight work.
pub struct SamClient<F: SamConnectionFactory> {
    factory: F,
    identity: SamIdentity,
    local_hash: [u8; 32],
    limits: SamLimits,
    timeouts: SamTimeouts,
    min_version: SamVersion,
    max_version: SamVersion,
    shutdown: Cancellation,
    state: Mutex<SessionState>,
}

impl<F: SamConnectionFactory> SamClient<F> {
    /// Oldest SAM version this client negotiates.
    pub const MIN_VERSION: SamVersion = SamVersion { major: 3, minor: 1 };
    /// Newest SAM version this client negotiates.
    pub const MAX_VERSION: SamVersion = SamVersion { major: 3, minor: 1 };

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
            local_hash: identity.hash(),
            identity,
            limits,
            timeouts,
            min_version: Self::MIN_VERSION,
            max_version: Self::MAX_VERSION,
            shutdown: Cancellation::default(),
            state: Mutex::new(SessionState {
                available: true,
                ..SessionState::default()
            }),
        })
    }

    pub fn session_id(&self) -> &str {
        self.identity.session_id()
    }

    pub fn identity(&self) -> &SamIdentity {
        &self.identity
    }

    pub fn local_destination_hash(&self) -> [u8; 32] {
        self.local_hash
    }

    pub fn limits(&self) -> SamLimits {
        self.limits
    }

    pub fn timeouts(&self) -> SamTimeouts {
        self.timeouts
    }

    /// Whether dependent stream operations may still be attempted.
    pub fn is_available(&self) -> bool {
        self.lock().map(|state| state.available).unwrap_or(false)
    }

    /// Whether the shared session has already been created by this client.
    pub fn is_session_created(&self) -> bool {
        self.lock().map(|state| state.created).unwrap_or(false)
    }

    /// The version the most recent connection negotiated, if any.
    pub fn negotiated_version(&self) -> Option<SamVersion> {
        self.lock().ok().and_then(|state| state.negotiated)
    }

    /// Number of operations currently holding a raw connection.
    pub fn in_flight(&self) -> usize {
        self.lock().map(|state| state.in_flight).unwrap_or(0)
    }

    /// Whether [`SamClient::close`] or a drop has cancelled this client.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    /// Marks the session unavailable and cancels in-flight work.
    ///
    /// This is the failure path a caller takes when it learns the shared SAM
    /// session is gone; it never destroys the injected identity, and it never
    /// retries on its own.
    pub fn mark_unavailable(&self) {
        if let Ok(mut state) = self.lock() {
            state.available = false;
            state.created = false;
        }
        self.shutdown.cancel();
    }

    /// Cancels in-flight work, marks the session unavailable, and waits a
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

    /// Negotiates and joins the session once, without retrying.
    ///
    /// Restoration is deliberately a single bounded attempt: a caller owns any
    /// re-establishment policy, so an unavailable SAM service cannot produce a
    /// reconnect storm here.
    pub async fn ensure_session(&self, cancellation: &Cancellation) -> Result<(), TransportError> {
        let _stream = self
            .open_connection(cancellation, self.timeouts.open, self.timeouts.handshake)
            .await?;
        Ok(())
    }

    /// Resolves a naming entry to a verified Destination.
    pub async fn nam_lookup(
        &self,
        name: &str,
        cancellation: &Cancellation,
    ) -> Result<identity::Destination, TransportError> {
        let command = encode_naming_lookup(name)?;
        let mut stream = self
            .open_connection(cancellation, self.timeouts.open, self.timeouts.handshake)
            .await?;
        let reply = self
            .exchange(
                &mut stream,
                &command,
                cancellation,
                self.timeouts.lookup,
                Some("DESTINATION"),
            )
            .await?;
        reply.require_ok()?;
        reply.destination("DESTINATION").map_err(Into::into)
    }

    /// Opens an outbound peer stream on a fresh raw SAM connection.
    ///
    /// The command always carries an explicit port; peer callers pass `0`,
    /// which the service reads as "no port". Nothing is special-cased away.
    pub async fn stream_connect(
        &self,
        destination: &identity::Destination,
        port: u16,
        cancellation: &Cancellation,
    ) -> Result<I2pStream, TransportError> {
        let command = encode_stream_connect(self.session_id(), destination.as_bytes(), port)?;
        let mut stream = self
            .open_connection(cancellation, self.timeouts.open, self.timeouts.handshake)
            .await?;
        self.exchange(
            &mut stream,
            &command,
            cancellation,
            self.timeouts.connect,
            None,
        )
        .await?
        .require_ok()?;
        Ok(stream)
    }

    /// Accepts one inbound peer stream on a fresh raw SAM connection.
    pub async fn stream_accept(
        &self,
        cancellation: &Cancellation,
    ) -> Result<(identity::Destination, I2pStream), TransportError> {
        let command = encode_stream_accept(self.session_id())?;
        let mut stream = self
            .open_connection(cancellation, self.timeouts.open, self.timeouts.handshake)
            .await?;
        let reply = self
            .exchange(
                &mut stream,
                &command,
                cancellation,
                self.timeouts.accept,
                Some("DESTINATION"),
            )
            .await?;
        reply.require_ok()?;
        let peer = reply.destination("DESTINATION")?;
        Ok((peer, stream))
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, SessionState>, TransportError> {
        self.state
            .lock()
            .map_err(|_| TransportError::Session("sam session state is poisoned".to_owned()))
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
        handshake_timeout: Duration,
    ) -> Result<SamRawStream, TransportError> {
        self.begin()?;
        let result = self
            .open_connection_inner(cancellation, open_timeout, handshake_timeout)
            .await;
        self.end();
        result
    }

    async fn open_connection_inner(
        &self,
        cancellation: &Cancellation,
        open_timeout: Duration,
        handshake_timeout: Duration,
    ) -> Result<SamRawStream, TransportError> {
        let mut stream = self
            .bounded(
                self.factory.open_sam_connection(),
                open_timeout,
                cancellation,
            )
            .await??;
        let version = self
            .handshake(&mut stream, cancellation, handshake_timeout)
            .await?;
        if let Ok(mut state) = self.lock() {
            state.negotiated = Some(version);
        }
        Ok(stream)
    }

    async fn handshake(
        &self,
        stream: &mut SamRawStream,
        cancellation: &Cancellation,
        timeout: Duration,
    ) -> Result<SamVersion, TransportError> {
        let command = encode_hello(self.min_version, self.max_version)?;
        let reply = self
            .exchange(stream, &command, cancellation, timeout, None)
            .await?;
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

        let command = encode_session_create(self.session_id())?;
        let reply = self
            .exchange(stream, &command, cancellation, timeout, None)
            .await?;
        if reply.kind() != SamReplyKind::Session {
            return Err(TransportError::Protocol);
        }
        let already_created = self.lock()?.created;
        match reply.result() {
            Some(SamResult::Ok) => {
                if let Ok(mut state) = self.lock() {
                    state.created = true;
                }
            }
            // A duplicated id is only benign for a session this client already
            // created; anything else is a real conflict with foreign state.
            Some(SamResult::DuplicatedId) if already_created => {}
            Some(SamResult::DuplicatedId) => {
                return Err(TransportError::Session(
                    "sam session id already exists and was not created here".to_owned(),
                ));
            }
            Some(other) => {
                return Err(TransportError::Session(format!(
                    "sam session create reported {other:?}"
                )));
            }
            None => return Err(TransportError::Protocol),
        }
        Ok(version)
    }

    async fn exchange(
        &self,
        stream: &mut SamRawStream,
        command: &[u8],
        cancellation: &Cancellation,
        timeout: Duration,
        want: Option<&'static str>,
    ) -> Result<SamReply, TransportError> {
        let limits = self.limits;
        self.bounded(
            async {
                stream.write_all(command).await?;
                stream.flush().await?;
                let mut budget = limits.max_reply_bytes;
                let raw_line = read_line(stream, &limits, &mut budget).await?;
                let mut reply = parse_status_line(trim_ending(&raw_line), &limits)?;
                // A following `KEY=VALUE` line is read only when the status line
                // omitted a value this command needs, so a stream that is
                // already raw data never blocks here.
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
                        Some(other) => {
                            return Err(TransportError::from(SamError::Router(other)));
                        }
                        None => {
                            return Err(TransportError::from(SamError::Protocol(
                                "SAM reply is missing RESULT",
                            )));
                        }
                    }
                }
                if reply.result.is_none() {
                    return Err(TransportError::from(SamError::Protocol(
                        "SAM reply is missing RESULT",
                    )));
                }
                Ok(reply)
            },
            timeout,
            cancellation,
        )
        .await?
    }
}

impl<F: SamConnectionFactory> Drop for SamClient<F> {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[async_trait]
impl<F: SamConnectionFactory> I2pSession for SamClient<F> {
    fn local_peer_hash(&self) -> [u8; 32] {
        self.local_hash
    }

    async fn lookup(&self, name: &str) -> Result<identity::Destination, TransportError> {
        self.nam_lookup(name, &Cancellation::default()).await
    }

    async fn connect(
        &self,
        destination: &identity::Destination,
        port: u16,
    ) -> Result<I2pStream, TransportError> {
        self.stream_connect(destination, port, &Cancellation::default())
            .await
    }

    async fn accept(&self) -> Result<(identity::Destination, I2pStream), TransportError> {
        self.stream_accept(&Cancellation::default()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};

    const SESSION_ID: &str = "torrent-session";
    /// Command octets the client must write.
    const HELLO: &str = "HELLO VERSION MIN=3.1 MAX=3.1\n";
    const CREATE: &str = "SESSION CREATE STYLE=STREAM ID=torrent-session\n";
    /// Reply octets a scripted service sends back.
    const HELLO_REPLY: &str = "HELLO REPLY RESULT=OK VERSION=3.1\n";
    const SESSION_REPLY: &str = "SESSION STATUS RESULT=OK\n";

    fn tiny_limits() -> SamLimits {
        SamLimits::strict()
    }

    fn test_timeouts() -> SamTimeouts {
        SamTimeouts {
            open: Duration::from_secs(2),
            handshake: Duration::from_secs(2),
            lookup: Duration::from_secs(2),
            connect: Duration::from_secs(2),
            accept: Duration::from_secs(2),
            close: Duration::from_secs(1),
        }
    }

    fn destination_b64(len: usize) -> String {
        encode_base64(&vec![0x5a; len])
    }

    fn client(replies: Vec<Vec<u8>>) -> (SamClient<ScriptedFactory>, Arc<Mutex<Vec<Transcript>>>) {
        let (factory, transcripts) = ScriptedFactory::new(replies);
        let identity = SamIdentity::new(None, SESSION_ID).unwrap();
        let client =
            SamClient::new(factory, identity, SamLimits::default(), test_timeouts()).unwrap();
        (client, transcripts)
    }

    trait I2pRawStreamHolder {
        fn raw_connection_count(&self) -> usize;
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct SamRawStreamHolder {
        opened: usize,
    }

    impl I2pRawStreamHolder for SamRawStreamHolder {
        fn raw_connection_count(&self) -> usize {
            self.opened
        }
    }

    /// One scripted connection: the exact octets the fake service writes back.
    #[derive(Clone, Debug, Default)]
    struct Transcript {
        written: Vec<u8>,
        finished: bool,
    }

    #[derive(Clone)]
    struct ScriptedFactory {
        script: Arc<Mutex<VecDeque<Vec<u8>>>>,
        transcripts: Arc<Mutex<Vec<Transcript>>>,
    }

    impl ScriptedFactory {
        fn new(replies: Vec<Vec<u8>>) -> (Self, Arc<Mutex<Vec<Transcript>>>) {
            let transcripts = Arc::new(Mutex::new(Vec::new()));
            let factory = Self {
                script: Arc::new(Mutex::new(replies.into())),
                transcripts: Arc::clone(&transcripts),
            };
            (factory, transcripts)
        }
    }

    #[async_trait]
    impl SamConnectionFactory for ScriptedFactory {
        async fn open_sam_connection(&self) -> Result<SamRawStream, TransportError> {
            let reply = self.script.lock().unwrap().pop_front().unwrap_or_default();
            let (client, server) = duplex(64 * 1024);
            let mut transcripts = self.transcripts.lock().unwrap();
            transcripts.push(Transcript {
                written: Vec::new(),
                finished: false,
            });
            let index = transcripts.len() - 1;
            drop(transcripts);
            let sink = Arc::clone(&self.transcripts);
            tokio::spawn(async move {
                let mut server = server;
                let _ = server.write_all(&reply).await;
                let _ = server.flush().await;
                let written = collect_written(&mut server).await;
                if let Ok(mut transcripts) = sink.lock() {
                    transcripts[index] = Transcript {
                        written,
                        finished: true,
                    };
                }
            });
            Ok(Box::pin(client))
        }
    }

    /// Reads every octet the client writes until it stops or the connection ends.
    async fn collect_written(server: &mut DuplexStream) -> Vec<u8> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut written = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let idle = remaining.min(Duration::from_millis(400));
            match tokio::time::timeout(idle, server.read(&mut byte)).await {
                Ok(Ok(0)) | Err(_) | Ok(Err(_)) => break,
                Ok(Ok(_)) => written.push(byte[0]),
            }
        }
        written
    }

    async fn finished_transcripts(
        transcripts: &Arc<Mutex<Vec<Transcript>>>,
        expected: usize,
    ) -> Vec<Transcript> {
        for _ in 0..300 {
            if transcripts.lock().unwrap().len() == expected
                && transcripts.lock().unwrap()[..expected]
                    .iter()
                    .all(|entry| entry.finished)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        transcripts.lock().unwrap()[..expected].to_vec()
    }

    #[test]
    fn sam_commands_are_exact_octets_and_never_encode_forwarding() {
        assert_eq!(
            encode_hello(
                SamClient::<ScriptedFactory>::MIN_VERSION,
                SamClient::<ScriptedFactory>::MAX_VERSION
            )
            .unwrap(),
            HELLO.as_bytes()
        );
        assert_eq!(
            encode_session_create(SESSION_ID).unwrap(),
            CREATE.as_bytes()
        );
        assert_eq!(
            encode_naming_lookup("tracker.i2p").unwrap(),
            b"NAMING LOOKUP NAME=tracker.i2p\n"
        );
        let destination = vec![0x5a; MIN_DESTINATION_BYTES];
        assert_eq!(
            encode_stream_connect(SESSION_ID, &destination, 0).unwrap(),
            format!(
                "STREAM CONNECT ID=torrent-session DESTINATION={} PORT=0 SILENT=false\n",
                destination_b64(MIN_DESTINATION_BYTES)
            )
            .as_bytes()
        );
        assert_eq!(
            encode_stream_accept(SESSION_ID).unwrap(),
            b"STREAM ACCEPT ID=torrent-session SILENT=false\n"
        );
        for encoded in [
            encode_hello(SamVersion::new(3, 1), SamVersion::new(3, 1)).unwrap(),
            encode_session_create(SESSION_ID).unwrap(),
            encode_naming_lookup("tracker.i2p").unwrap(),
            encode_stream_connect(SESSION_ID, &destination, 0).unwrap(),
            encode_stream_accept(SESSION_ID).unwrap(),
        ] {
            let text = String::from_utf8(encoded.clone()).unwrap();
            assert!(text.ends_with('\n'), "{text} lost its line terminator");
            assert!(!text.contains("FORWARD"), "{text} encoded forwarding");
            assert!(text.lines().all(|line| !line.contains("  ")));
        }
    }

    #[test]
    fn sam_command_tokens_reject_ambiguous_injection() {
        assert!(matches!(encode_session_create(""), Err(SamError::Limit)));
        assert!(matches!(
            encode_naming_lookup("name with space"),
            Err(SamError::Protocol(_))
        ));
        assert!(matches!(
            encode_naming_lookup("name\r\nHELLO"),
            Err(SamError::Protocol(_))
        ));
        assert!(matches!(
            encode_stream_accept(&"a".repeat(MAX_COMMAND_TOKEN_BYTES + 1)),
            Err(SamError::Limit)
        ));
        assert!(matches!(
            encode_stream_connect(SESSION_ID, &vec![0; MAX_DESTINATION_BYTES + 1], 0),
            Err(SamError::Limit)
        ));
    }

    #[test]
    fn sam_base64_is_standard_padded_and_rejects_bad_input() {
        assert_eq!(encode_base64(b"a"), "YQ==");
        assert_eq!(encode_base64(b"ab"), "YWI=");
        assert_eq!(encode_base64(b"abc"), "YWJj");
        assert_eq!(decode_base64(b"YWJj", 3).unwrap(), b"abc");
        assert_eq!(decode_base64(b"YQ==", 1).unwrap(), b"a");
        for bad in [
            &b"YQ="[..],
            &b"YQ==="[..],
            &b"YQ==YQ=="[..],
            &b"Y*Jj"[..],
            &b"YW_j"[..],
        ] {
            assert!(matches!(decode_base64(bad, 64), Err(SamError::Protocol(_))));
        }
        assert!(matches!(decode_base64(b"YWJj", 2), Err(SamError::Limit)));
        let destination = destination_b64(MIN_DESTINATION_BYTES);
        assert_eq!(
            decode_base64(destination.as_bytes(), MAX_DESTINATION_BYTES)
                .unwrap()
                .len(),
            MIN_DESTINATION_BYTES
        );
    }

    #[test]
    fn sam_reply_parsing_accepts_status_plus_option_lines() {
        let reply = parse_reply_block(
            format!(
                "NAMING REPLY RESULT=OK\nDESTINATION={}\n",
                destination_b64(MIN_DESTINATION_BYTES)
            )
            .as_bytes(),
            &SamLimits::default(),
        )
        .unwrap();
        assert_eq!(reply.kind(), SamReplyKind::Naming);
        assert_eq!(reply.result(), Some(SamResult::Ok));
        assert_eq!(
            reply.destination("DESTINATION").unwrap().as_bytes().len(),
            MIN_DESTINATION_BYTES
        );
        let inline = parse_reply_block(
            format!(
                "STREAM STATUS RESULT=OK DESTINATION={}\n",
                destination_b64(MIN_DESTINATION_BYTES)
            )
            .as_bytes(),
            &SamLimits::default(),
        )
        .unwrap();
        assert_eq!(inline.kind(), SamReplyKind::Stream);
        assert!(inline.option("DESTINATION").is_some());
        let hello = parse_reply_block(
            b"HELLO REPLY RESULT=OK VERSION=3.1\n",
            &SamLimits::default(),
        )
        .unwrap();
        assert_eq!(hello.kind(), SamReplyKind::Hello);
        assert_eq!(
            SamVersion::parse(hello.option("VERSION").unwrap()).unwrap(),
            SamVersion::new(3, 1)
        );
        assert!(matches!(reply.require_ok(), Ok(())));
    }

    #[test]
    fn sam_reply_parsing_rejects_malformed_and_oversized_lines() {
        let limits = SamLimits::default();
        let cases: [(&str, SamError); 8] = [
            (
                "HELLO REPLY VERSION=3.1\n",
                SamError::Protocol("SAM reply is missing RESULT"),
            ),
            (
                "HELLO REPLY RESULT=MAYBE\n",
                SamError::Protocol("unknown SAM result"),
            ),
            (
                "HELLO STATUS RESULT=OK\n",
                SamError::Protocol("unknown SAM reply kind"),
            ),
            (
                "FROBNICATE REPLY RESULT=OK\n",
                SamError::Protocol("unknown SAM reply kind"),
            ),
            (
                "HELLO REPLY RESULT=OK =value\n",
                SamError::Protocol("SAM option key is empty"),
            ),
            (
                "HELLO REPLY RESULT=OK BROKEN\n",
                SamError::Protocol("SAM option is missing a value"),
            ),
            (
                "HELLO REPLY RESULT=OK NAME=\"unterminated\n",
                SamError::Protocol("unterminated quoted SAM value"),
            ),
            (
                "HELLO REPLY RESULT=OK NAME=\"a\"junk\n",
                SamError::Protocol("trailing bytes after a quoted SAM value"),
            ),
        ];
        for (input, expected) in cases {
            let Err(outcome) = parse_reply_block(input.as_bytes(), &limits) else {
                panic!("malformed input must be rejected: {input:?}");
            };
            assert_eq!(
                format!("{outcome:?}"),
                format!("{expected:?}"),
                "unexpected parsing outcome for {input:?}"
            );
        }
        let small = tiny_limits();
        let long_line = format!("HELLO REPLY RESULT=OK VERSION={}\n", "a".repeat(200));
        assert!(matches!(
            parse_reply_block(long_line.as_bytes(), &small),
            Err(SamError::Limit)
        ));
        let long_value = format!("HELLO REPLY RESULT=OK NAME={}\n", "a".repeat(64));
        assert!(matches!(
            parse_reply_block(long_value.as_bytes(), &small),
            Err(SamError::Limit)
        ));
        let unquoted_space = "HELLO REPLY RESULT=OK NAME=two words\n";
        assert!(matches!(
            parse_reply_block(unquoted_space.as_bytes(), &SamLimits::default()),
            Err(SamError::Protocol(_))
        ));
        let escaped =
            parse_reply_block(b"HELLO REPLY RESULT=OK NAME=\"a\\\"b\\\\c\"\n", &limits).unwrap();
        assert_eq!(escaped.option("NAME"), Some("a\"b\\c"));
        assert!(matches!(
            parse_reply_block(b"HELLO REPLY RESULT=OK NAME=\"a\\qb\"\n", &limits),
            Err(SamError::Protocol(_))
        ));
    }

    #[test]
    fn sam_reply_parsing_bounds_option_count_and_rejects_duplicates() {
        let limits = tiny_limits();
        let mut block = String::from("HELLO REPLY RESULT=OK\n");
        for index in 0..8 {
            block.push_str(&format!("KEY{index}=value\n"));
        }
        assert!(matches!(
            parse_reply_block(block.as_bytes(), &limits),
            Err(SamError::Limit)
        ));
        let duplicate = "HELLO REPLY RESULT=OK VERSION=3.1 VERSION=3.1\n";
        assert!(matches!(
            parse_reply_block(duplicate.as_bytes(), &SamLimits::default()),
            Err(SamError::Protocol(_))
        ));
        assert!(matches!(SamLimits::default().validate(), Ok(())));
        for broken in [
            SamLimits {
                max_token_bytes: 2,
                ..tiny_limits()
            },
            SamLimits {
                max_options: 0,
                ..tiny_limits()
            },
            SamLimits {
                max_line_bytes: 4,
                ..tiny_limits()
            },
            SamLimits {
                max_reply_bytes: 8,
                ..tiny_limits()
            },
        ] {
            assert!(matches!(broken.validate(), Err(SamError::Configuration)));
        }
    }

    #[test]
    fn sam_reply_parsing_rejects_out_of_range_destinations() {
        let limits = SamLimits::default();
        let short = format!(
            "NAMING REPLY RESULT=OK DESTINATION={}\n",
            destination_b64(MIN_DESTINATION_BYTES - 1)
        );
        let outcome = parse_reply_block(short.as_bytes(), &limits)
            .and_then(|reply| reply.destination("DESTINATION"));
        assert!(
            matches!(outcome, Err(SamError::Protocol(_))),
            "a 386-byte destination must be rejected as a protocol violation"
        );
        let long = format!(
            "NAMING REPLY RESULT=OK DESTINATION={}\n",
            destination_b64(MAX_DESTINATION_BYTES + 1)
        );
        assert!(matches!(
            parse_reply_block(long.as_bytes(), &limits)
                .and_then(|reply| reply.destination("DESTINATION")),
            Err(SamError::Limit)
        ));
        for bad in ["QUJD", "****", "YQ==YQ=="] {
            let block = format!("NAMING REPLY RESULT=OK DESTINATION={bad}\n");
            assert!(
                parse_reply_block(block.as_bytes(), &limits)
                    .and_then(|reply| reply.destination("DESTINATION"))
                    .is_err()
            );
        }
        assert!(matches!(
            parse_reply_block(b"NAMING REPLY RESULT=OK\n".as_slice(), &limits)
                .and_then(|reply| reply.destination("DESTINATION")),
            Err(SamError::Missing("DESTINATION"))
        ));
    }

    #[test]
    fn sam_reply_parsing_rejects_non_decimal_and_overflowing_numbers() {
        for bad in ["3", "3.x", "x.1", "3.1.0", "42949672960.0", "-1.0", ""] {
            assert!(
                matches!(SamVersion::parse(bad), Err(SamError::Protocol(_))),
                "accepted {bad:?}"
            );
        }
        assert_eq!(SamVersion::parse("3.1").unwrap(), SamVersion::new(3, 1));
        assert!(SamVersion::new(3, 1).within(SamVersion::new(3, 1), SamVersion::new(3, 1)));
        assert!(!SamVersion::new(3, 2).within(SamVersion::new(3, 1), SamVersion::new(3, 1)));
        assert!(SamVersion::new(4, 0).to_string() == "4.0");
    }

    #[test]
    fn sam_oversized_reply_block_is_rejected_at_its_bound() {
        let limits = SamLimits {
            max_line_bytes: 64,
            max_reply_bytes: 256,
            max_token_bytes: 16,
            max_value_bytes: 32,
            max_options: 4,
        };
        let oversized = vec![b'A'; 64 * 1024];
        assert!(matches!(
            parse_reply_block(&oversized, &limits),
            Err(SamError::Limit)
        ));
        let mut many_lines = b"HELLO REPLY RESULT=OK\n".to_vec();
        for index in 0..64 {
            many_lines.extend_from_slice(format!("K{index}=v\n").as_bytes());
        }
        assert!(matches!(
            parse_reply_block(&many_lines, &limits),
            Err(SamError::Limit)
        ));
    }

    #[test]
    fn sam_identity_makes_the_key_boundary_explicit() {
        let encoded = destination_b64(MIN_DESTINATION_BYTES);
        let identity = SamIdentity::new(Some(&encoded), SESSION_ID).unwrap();
        assert!(identity.has_destination());
        assert_eq!(identity.session_id(), SESSION_ID);
        let expected: [u8; 32] = sha2::Sha256::digest(vec![0x5a; MIN_DESTINATION_BYTES]).into();
        assert_eq!(identity.hash(), expected);
        let anonymous = SamIdentity::new(None, SESSION_ID).unwrap();
        assert!(!anonymous.has_destination());
        assert_ne!(anonymous.hash(), identity.hash());
        assert_eq!(anonymous.hash(), session_identity_hash(SESSION_ID));
        assert!(matches!(
            SamIdentity::new(Some("not base64!"), SESSION_ID),
            Err(SamError::Protocol(_))
        ));
        assert!(matches!(
            SamIdentity::new(Some(&destination_b64(64)), SESSION_ID),
            Err(SamError::Protocol(_))
        ));
        assert!(matches!(
            SamIdentity::new(None, "bad id"),
            Err(SamError::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn sam_hello_negotiates_once_per_raw_connection() {
        let (client, transcripts) = client(vec![
            b"HELLO REPLY RESULT=OK VERSION=3.1\nSESSION STATUS RESULT=OK\n".to_vec(),
            b"HELLO REPLY RESULT=OK VERSION=3.1\nSESSION STATUS RESULT=OK\n".to_vec(),
        ]);
        client
            .ensure_session(&Cancellation::default())
            .await
            .unwrap();
        client
            .ensure_session(&Cancellation::default())
            .await
            .unwrap();
        let entries = finished_transcripts(&transcripts, 2).await;
        for entry in &entries {
            assert_eq!(
                String::from_utf8(entry.written.clone()).unwrap(),
                format!("{HELLO}{CREATE}")
            );
        }
        assert_eq!(client.negotiated_version(), Some(SamVersion::new(3, 1)));
        assert!(client.is_session_created());
    }

    #[tokio::test]
    async fn sam_hello_rejects_no_version_and_unsupported_versions() {
        let (client, transcripts) = client(vec![
            b"HELLO REPLY RESULT=NOVERSION\n".to_vec(),
            b"HELLO REPLY RESULT=OK VERSION=9.9\n".to_vec(),
            b"HELLO REPLY RESULT=OK\n".to_vec(),
            b"HELLO REPLY RESULT=OK VERSION=3.x\n".to_vec(),
        ]);
        // `NOVERSION` and an unsupported advertised version are router
        // outcomes; a missing or unparsable version is a protocol violation.
        for session_failure in [true, true] {
            let outcome = client.ensure_session(&Cancellation::default()).await.err();
            assert!(
                matches!(outcome, Some(TransportError::Session(_))) == session_failure,
                "{outcome:?}"
            );
        }
        for protocol_failure in [true, true] {
            let outcome = client.ensure_session(&Cancellation::default()).await.err();
            assert!(
                matches!(outcome, Some(TransportError::Protocol)) == protocol_failure,
                "{outcome:?}"
            );
        }
        let entries = finished_transcripts(&transcripts, 4).await;
        assert_eq!(entries.len(), 4);
        for entry in &entries {
            assert_eq!(entry.written, HELLO.as_bytes());
        }
        assert!(!client.is_session_created());
    }

    #[tokio::test]
    async fn sam_session_create_reuses_one_id_and_tolerates_duplicates_it_created() {
        let (client, transcripts) = client(vec![
            b"HELLO REPLY RESULT=OK VERSION=3.1\nSESSION STATUS RESULT=OK\n".to_vec(),
            b"HELLO REPLY RESULT=OK VERSION=3.1\nSESSION STATUS RESULT=DUPLICATED_ID\n".to_vec(),
        ]);
        client
            .ensure_session(&Cancellation::default())
            .await
            .unwrap();
        assert!(client.is_session_created());
        client
            .ensure_session(&Cancellation::default())
            .await
            .unwrap();
        let entries = finished_transcripts(&transcripts, 2).await;
        for entry in &entries {
            assert_eq!(entry.written, format!("{HELLO}{CREATE}").into_bytes());
        }
        assert_eq!(client.session_id(), SESSION_ID);
    }

    #[tokio::test]
    async fn sam_duplicated_id_from_foreign_state_is_a_session_error() {
        let (client, transcripts) = client(vec![
            b"HELLO REPLY RESULT=OK VERSION=3.1\nSESSION STATUS RESULT=DUPLICATED_ID\n".to_vec(),
        ]);
        assert!(matches!(
            client.ensure_session(&Cancellation::default()).await,
            Err(TransportError::Session(_))
        ));
        assert!(!client.is_session_created());
        finished_transcripts(&transcripts, 1).await;
    }

    #[tokio::test]
    async fn sam_session_create_maps_failure_results_to_session_errors() {
        let (client, transcripts) = client(vec![
            b"HELLO REPLY RESULT=OK VERSION=3.1\nSESSION STATUS RESULT=INVALID_KEY\n".to_vec(),
        ]);
        assert!(matches!(
            client.ensure_session(&Cancellation::default()).await,
            Err(TransportError::Session(_))
        ));
        finished_transcripts(&transcripts, 1).await;
    }

    #[tokio::test]
    async fn sam_naming_lookup_returns_the_destination_from_a_fresh_connection() {
        let destination = vec![0x21; MIN_DESTINATION_BYTES];
        let (client, transcripts) = client(vec![format!(
            "HELLO REPLY RESULT=OK VERSION=3.1\nSESSION STATUS RESULT=OK\nNAMING REPLY RESULT=OK\nDESTINATION={}\n",
            encode_base64(&destination)
        )
        .into_bytes()]);
        let resolved = client
            .nam_lookup("tracker.i2p", &Cancellation::default())
            .await
            .unwrap();
        assert_eq!(resolved.as_bytes(), destination.as_slice());
        let entries = finished_transcripts(&transcripts, 1).await;
        assert_eq!(
            String::from_utf8(entries[0].written.clone()).unwrap(),
            format!("{HELLO}{CREATE}NAMING LOOKUP NAME=tracker.i2p\n")
        );
    }

    #[tokio::test]
    async fn sam_naming_lookup_maps_failures_and_malformed_replies() {
        let cases = [
            (
                format!("{HELLO_REPLY}{SESSION_REPLY}NAMING REPLY RESULT=KEY_NOT_FOUND\n"),
                true,
            ),
            (
                format!("{HELLO_REPLY}{SESSION_REPLY}NAMING REPLY RESULT=OK\nDESTINATION=****\n"),
                false,
            ),
            (
                format!(
                    "{HELLO_REPLY}{SESSION_REPLY}NAMING REPLY RESULT=OK DESTINATION={}\n",
                    destination_b64(64)
                ),
                false,
            ),
        ];
        for (reply, expect_session_error) in cases {
            let transcript = reply.clone();
            let (client, transcripts) = client(vec![reply.into_bytes()]);
            let outcome = client
                .nam_lookup("missing.i2p", &Cancellation::default())
                .await
                .err();
            assert_eq!(
                matches!(outcome, Some(TransportError::Session(_))),
                expect_session_error,
                "unexpected mapping for {transcript:?}"
            );
            assert!(outcome.is_some(), "{transcript:?} unexpectedly succeeded");
            finished_transcripts(&transcripts, 1).await;
        }
    }

    #[tokio::test]
    async fn sam_stream_connect_carries_an_explicit_port_and_returns_raw_bytes() {
        let peer = vec![0x31; MIN_DESTINATION_BYTES];
        let (client, transcripts) = client(vec![
            b"HELLO REPLY RESULT=OK VERSION=3.1\nSESSION STATUS RESULT=OK\n".to_vec(),
            [
                format!("{HELLO_REPLY}{SESSION_REPLY}STREAM STATUS RESULT=OK\n").into_bytes(),
                b"\x13bitfield".to_vec(),
            ]
            .concat(),
        ]);
        let destination =
            identity::Destination::from_bytes(vec![0x41; MIN_DESTINATION_BYTES]).unwrap();
        client
            .ensure_session(&Cancellation::default())
            .await
            .unwrap();
        let mut stream = client
            .stream_connect(&destination, 0, &Cancellation::default())
            .await
            .unwrap();
        stream.write_all(b"\x13bitfield").await.unwrap();
        let mut sink = vec![0u8; 9];
        tokio::io::AsyncReadExt::read_exact(&mut stream, &mut sink)
            .await
            .unwrap();
        assert_eq!(&sink, b"\x13bitfield");
        drop(stream);
        let entries = finished_transcripts(&transcripts, 2).await;
        // The scripted collector keeps draining after the status line, so the
        // client's raw payload write is captured too; the command is a prefix.
        let expected_command = format!(
            "{HELLO}{CREATE}STREAM CONNECT ID=torrent-session DESTINATION={} PORT=0 SILENT=false\n",
            encode_base64(destination.as_bytes())
        );
        assert!(
            String::from_utf8(entries[1].written.clone())
                .unwrap()
                .starts_with(&expected_command),
            "unexpected octets after the connect command"
        );
        assert_eq!(peer.len(), MIN_DESTINATION_BYTES);
    }

    #[tokio::test]
    async fn sam_stream_connect_maps_failure_results() {
        for result in [
            "CANT_REACH_PEER",
            "INVALID_ID",
            "I2P_ERROR",
            "NOT_IMPLEMENTED",
        ] {
            let (client, transcripts) = client(vec![
                format!("{HELLO_REPLY}{SESSION_REPLY}STREAM STATUS RESULT={result}\n").into_bytes(),
            ]);
            let destination =
                identity::Destination::from_bytes(vec![0x41; MIN_DESTINATION_BYTES]).unwrap();
            let outcome = client
                .stream_connect(&destination, 0, &Cancellation::default())
                .await
                .err();
            assert!(
                matches!(outcome, Some(TransportError::Session(_))),
                "result {result}: {outcome:?}"
            );
            finished_transcripts(&transcripts, 1).await;
        }
    }

    #[tokio::test]
    async fn sam_stream_accept_consumes_the_destination_line_then_raw_data() {
        let peer = vec![0x77; MIN_DESTINATION_BYTES];
        let (client, transcripts) = client(vec![
            [
                format!(
                    "{HELLO_REPLY}{SESSION_REPLY}STREAM STATUS RESULT=OK DESTINATION={}\n",
                    encode_base64(&peer)
                )
                .into_bytes(),
                vec![1, 2, 3, 4],
            ]
            .concat(),
        ]);
        let (destination, mut stream) = client
            .stream_accept(&Cancellation::default())
            .await
            .unwrap();
        assert_eq!(destination.as_bytes(), peer.as_slice());
        let mut payload = vec![0u8; 4];
        tokio::io::AsyncReadExt::read_exact(&mut stream, &mut payload)
            .await
            .unwrap();
        assert_eq!(payload, vec![1, 2, 3, 4]);
        drop(stream);
        let entries = finished_transcripts(&transcripts, 1).await;
        assert_eq!(
            String::from_utf8(entries[0].written.clone()).unwrap(),
            format!("{HELLO}{CREATE}STREAM ACCEPT ID=torrent-session SILENT=false\n")
        );
    }

    #[tokio::test]
    async fn sam_stream_accept_is_cancellable_while_blocked() {
        let (client, transcripts) =
            client(vec![format!("{HELLO_REPLY}{SESSION_REPLY}").into_bytes()]);
        let cancellation = Cancellation::default();
        let token = cancellation.clone();
        let owned = Arc::new(client);
        let blocked = {
            let owned = Arc::clone(&owned);
            async move { owned.stream_accept(&token).await }
        };
        let handle =
            tokio::spawn(
                async move { tokio::time::timeout(Duration::from_millis(80), blocked).await },
            );
        tokio::time::sleep(Duration::from_millis(30)).await;
        cancellation.cancel();
        let outcome = handle.await.unwrap();
        drop(owned);
        assert!(
            matches!(outcome, Ok(Err(TransportError::Cancelled))),
            "expected a bounded cancellation, got a different outcome"
        );
        finished_transcripts(&transcripts, 1).await;
    }

    #[tokio::test]
    async fn sam_reply_timeout_and_eof_are_bounded_failures() {
        let (client, transcripts) = client(vec![
            format!("{HELLO_REPLY}{SESSION_REPLY}").into_bytes(),
            format!("{HELLO_REPLY}{SESSION_REPLY}NAMING REPLY RESULT=OK\nDEST").into_bytes(),
        ]);
        let slow_identity = SamIdentity::new(None, SESSION_ID).unwrap();
        let slow = SamClient::new(
            {
                let (factory, _sink) = ScriptedFactory::new(vec![
                    format!("{HELLO_REPLY}{SESSION_REPLY}").into_bytes(),
                ]);
                factory
            },
            slow_identity,
            SamLimits::default(),
            SamTimeouts {
                handshake: Duration::from_millis(60),
                lookup: Duration::from_millis(120),
                connect: Duration::from_millis(120),
                accept: Duration::from_millis(120),
                ..test_timeouts()
            },
        );
        let slow = slow.expect("bounded SAM client limits are valid");
        let timed = slow
            .nam_lookup("tracker.i2p", &Cancellation::default())
            .await;
        assert!(matches!(timed, Err(TransportError::Timeout)), "{timed:?}");
        assert!(matches!(
            client
                .nam_lookup("tracker.i2p", &Cancellation::default())
                .await
                .err(),
            Some(TransportError::Io(_))
        ));
        finished_transcripts(&transcripts, 1).await;
    }

    #[tokio::test]
    async fn sam_close_cancels_in_flight_work_and_fails_dependent_operations() {
        let (client, transcripts) =
            client(vec![format!("{HELLO_REPLY}{SESSION_REPLY}").into_bytes()]);
        client
            .ensure_session(&Cancellation::default())
            .await
            .unwrap();
        assert!(client.is_available());
        let blocking = tokio::time::sleep(Duration::from_secs(30));
        let token = Cancellation::default();
        let joined = tokio::join!(client.ensure_session(&token), client.close());
        let outcome = tokio::time::timeout(Duration::from_secs(1), async { joined })
            .await
            .unwrap_or_else(|_| panic!("close did not release in-flight work within its bound"));
        drop(blocking);
        let (attempt, close) = outcome;
        assert!(matches!(attempt, Err(TransportError::Cancelled)));
        assert!(close.is_ok(), "close did not finish cleanly");
        assert!(!client.is_available());
        assert!(client.is_shutdown());
        assert!(matches!(
            client.ensure_session(&Cancellation::default()).await,
            Err(TransportError::Cancelled)
        ));
        finished_transcripts(&transcripts, 2).await;
    }

    #[tokio::test]
    async fn sam_i2p_session_adapter_drives_lookup_connect_and_accept() {
        let peer = vec![0x63; MIN_DESTINATION_BYTES];
        let (client, transcripts) = client(vec![
            format!(
                "{HELLO_REPLY}{SESSION_REPLY}NAMING REPLY RESULT=OK DESTINATION={}\n",
                encode_base64(&peer)
            )
            .into_bytes(),
            format!("{HELLO_REPLY}{SESSION_REPLY}STREAM STATUS RESULT=OK\n").into_bytes(),
            format!(
                "{HELLO_REPLY}{SESSION_REPLY}STREAM STATUS RESULT=OK DESTINATION={}\n",
                encode_base64(&peer)
            )
            .into_bytes(),
        ]);
        let session: &dyn I2pSession = &client;
        assert_eq!(session.local_peer_hash(), client.local_destination_hash());
        let resolved = session.lookup("peer.i2p").await.unwrap();
        assert_eq!(resolved.as_bytes(), peer.as_slice());
        drop(session.connect(&resolved, 0).await.unwrap());
        let (accepted, stream) = session.accept().await.unwrap();
        assert_eq!(accepted.as_bytes(), peer.as_slice());
        drop(stream);
        let entries = finished_transcripts(&transcripts, 3).await;
        let commands: Vec<String> = entries
            .iter()
            .map(|entry| String::from_utf8(entry.written.clone()).unwrap())
            .collect();
        assert_eq!(
            commands,
            vec![
                format!("{HELLO}{CREATE}NAMING LOOKUP NAME=peer.i2p\n"),
                format!(
                    "{HELLO}{CREATE}STREAM CONNECT ID=torrent-session DESTINATION={} PORT=0 SILENT=false\n",
                    encode_base64(&peer)
                ),
                format!("{HELLO}{CREATE}STREAM ACCEPT ID=torrent-session SILENT=false\n"),
            ]
        );
        let forbidden: BTreeSet<&str> = commands
            .iter()
            .flat_map(|text| text.lines())
            .filter(|line| line.contains("FORWARD"))
            .collect();
        assert!(forbidden.is_empty());
    }

    #[tokio::test]
    async fn sam_i2p_session_adapter_reports_unavailable_transport() {
        let (client, _) = client(vec![Vec::new(); 4]);
        client.mark_unavailable();
        let session: &dyn I2pSession = &client;
        assert!(matches!(
            session.lookup("peer.i2p").await,
            Err(TransportError::Cancelled)
        ));
        assert!(matches!(
            session
                .connect(
                    &identity::Destination::from_bytes(vec![1; MIN_DESTINATION_BYTES]).unwrap(),
                    0
                )
                .await,
            Err(TransportError::Cancelled)
        ));
        assert!(matches!(
            session.accept().await,
            Err(TransportError::Cancelled)
        ));
    }

    #[test]
    fn sam_source_obeys_the_static_boundary_rules() {
        let source = include_str!("sam.rs");
        let forbidden = [
            ["std", "net"].join("::"),
            ["tokio", "net"].join("::"),
            ["Tcp", "Stream"].concat(),
            ["Tcp", "Listener"].concat(),
            ["Udp", "Socket"].concat(),
            ["std", "process", "Command"].join("::"),
            format!("i2pr_{}", "daemon"),
            format!("i2pr_{}", "router"),
            format!("i2pr_{}", "sam"),
            format!("i2pr_{}", "proto"),
            format!("i2pr_{}", "app"),
            format!("i2pr_{}", "runtime"),
        ];
        for token in forbidden {
            assert!(
                !source.to_lowercase().contains(&token.to_lowercase()),
                "boundary token {token} leaked into sam.rs"
            );
        }
    }

    #[test]
    fn sam_raw_stream_holder_reports_connection_counts() {
        let holder = SamRawStreamHolder { opened: 3 };
        assert_eq!(holder.raw_connection_count(), 3);
    }
}
