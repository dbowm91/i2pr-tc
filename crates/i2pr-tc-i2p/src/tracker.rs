//! Bounded HTTP I2P tracker announces over an injected Destination stream.
use crate::{
    identity::{self, I2pPeer},
    I2pSession, TransportError,
};
use i2pr_tc_core::{bencode, service::TorrentService, InfoHashV1};
use i2pr_tc_storage::{Cancellation, PersistentTorrentService};
use std::{future::Future, time::Duration};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_REQUEST_BYTES: usize = 16 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_HEADER_BYTES: usize = 32 * 1024;

#[derive(Debug, Error)]
pub enum TrackerError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("I2P stream I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid I2P tracker URL or response")]
    Protocol,
    #[error("tracker response exceeded its bound")]
    Limit,
    #[error("tracker returned failure: {0}")]
    Failure(String),
    #[error("torrent service rejected tracker lookup")]
    Service,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackerEndpoint {
    pub host: String,
    pub port: u16,
    pub path_and_query: String,
}

impl TrackerEndpoint {
    pub fn parse(value: &str) -> Result<Self, TrackerError> {
        let rest = value
            .strip_prefix("http://")
            .ok_or(TrackerError::Protocol)?;
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let authority = &rest[..authority_end];
        if authority.is_empty() || authority.contains('@') || authority.contains(['[', ']']) {
            return Err(TrackerError::Protocol);
        }
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) if !host.contains(':') => (
                host,
                port.parse::<u16>().map_err(|_| TrackerError::Protocol)?,
            ),
            Some(_) => return Err(TrackerError::Protocol),
            None => (authority, 80),
        };
        if port == 0 {
            return Err(TrackerError::Protocol);
        }
        identity::validate_i2p_hostname(host).map_err(|_| TrackerError::Protocol)?;
        let suffix = &rest[authority_end..];
        if suffix.contains('#') || suffix.contains(['\r', '\n']) {
            return Err(TrackerError::Protocol);
        }
        let path_and_query = if suffix.is_empty() {
            "/announce".to_owned()
        } else if suffix.starts_with('?') {
            format!("/announce{suffix}")
        } else {
            suffix.to_owned()
        };
        if !path_and_query.starts_with('/') {
            return Err(TrackerError::Protocol);
        }
        Ok(Self {
            host: host.to_owned(),
            port,
            path_and_query,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnnounceEvent {
    Started,
    Stopped,
    Completed,
}

impl AnnounceEvent {
    fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Stopped => "stopped",
            Self::Completed => "completed",
        }
    }
}

#[derive(Clone, Debug)]
pub struct AnnounceRequest {
    pub info_hash: InfoHashV1,
    pub peer_id: [u8; 20],
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub numwant: u16,
    pub event: Option<AnnounceEvent>,
}

#[derive(Clone, Copy, Debug)]
pub struct TrackerLimits {
    pub response_bytes: usize,
    pub peers: usize,
    pub timeout: Duration,
}

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub attempts_per_tracker: u8,
    pub initial_delay: Duration,
    pub maximum_delay: Duration,
    pub jitter: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts_per_tracker: 2,
            initial_delay: Duration::from_secs(2),
            maximum_delay: Duration::from_secs(60),
            jitter: Duration::from_millis(500),
        }
    }
}

impl Default for TrackerLimits {
    fn default() -> Self {
        Self {
            response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            peers: 512,
            timeout: Duration::from_secs(90),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnnounceResponse {
    pub interval: Duration,
    pub min_interval: Option<Duration>,
    pub complete: Option<u32>,
    pub incomplete: Option<u32>,
    pub peers: Vec<I2pPeer>,
    pub warning: Option<String>,
}

pub async fn announce<S: I2pSession + ?Sized>(
    session: &S,
    tracker_url: &str,
    request: &AnnounceRequest,
    limits: TrackerLimits,
) -> Result<AnnounceResponse, TrackerError> {
    announce_inner(session, tracker_url, request, limits, None).await
}

pub async fn announce_cancellable<S: I2pSession + ?Sized>(
    session: &S,
    tracker_url: &str,
    request: &AnnounceRequest,
    limits: TrackerLimits,
    cancellation: &Cancellation,
) -> Result<AnnounceResponse, TrackerError> {
    announce_inner(session, tracker_url, request, limits, Some(cancellation)).await
}

async fn announce_inner<S: I2pSession + ?Sized>(
    session: &S,
    tracker_url: &str,
    request: &AnnounceRequest,
    limits: TrackerLimits,
    cancellation: Option<&Cancellation>,
) -> Result<AnnounceResponse, TrackerError> {
    if limits.response_bytes == 0
        || limits.response_bytes > DEFAULT_MAX_RESPONSE_BYTES
        || limits.peers > 4096
        || limits.timeout.is_zero()
        || request.numwant as usize > limits.peers
    {
        return Err(TrackerError::Limit);
    }
    let endpoint = TrackerEndpoint::parse(tracker_url)?;
    let destination =
        bounded_transport(session.lookup(&endpoint.host), limits.timeout, cancellation).await?;
    let mut stream = bounded_transport(
        session.connect(&destination, endpoint.port),
        limits.timeout,
        cancellation,
    )
    .await?;
    let request_bytes = build_request(&endpoint, request)?;
    let operation = async {
        stream.write_all(&request_bytes).await?;
        stream.flush().await?;
        let response = read_http_response(&mut stream, limits.response_bytes).await?;
        parse_announce_response(&response, limits.peers)
    };
    let operation = tokio::time::timeout(limits.timeout, operation);
    if let Some(token) = cancellation {
        race_cancel(operation, token)
            .await?
            .map_err(|_| TrackerError::Transport(TransportError::Timeout))?
    } else {
        operation
            .await
            .map_err(|_| TrackerError::Transport(TransportError::Timeout))?
    }
}

async fn bounded_transport<F, T>(
    operation: F,
    timeout: Duration,
    cancellation: Option<&Cancellation>,
) -> Result<T, TrackerError>
where
    F: Future<Output = Result<T, TransportError>>,
{
    let operation = tokio::time::timeout(timeout, operation);
    let result = if let Some(token) = cancellation {
        race_cancel(operation, token)
            .await?
            .map_err(|_| TrackerError::Transport(TransportError::Timeout))?
    } else {
        operation
            .await
            .map_err(|_| TrackerError::Transport(TransportError::Timeout))?
    };
    result.map_err(TrackerError::Transport)
}

/// Try trackers in tier order, retrying each with capped exponential backoff.
/// Unsupported targets fail validation before the injected session can connect.
pub async fn announce_with_failover<S: I2pSession + ?Sized>(
    session: &S,
    tiers: &[Vec<String>],
    request: &AnnounceRequest,
    limits: TrackerLimits,
    retry: RetryPolicy,
    cancellation: &Cancellation,
) -> Result<(String, AnnounceResponse), TrackerError> {
    if retry.attempts_per_tracker == 0
        || retry.initial_delay > retry.maximum_delay
        || retry.maximum_delay > Duration::from_secs(300)
        || retry.jitter > Duration::from_secs(30)
    {
        return Err(TrackerError::Limit);
    }
    let mut last_error = None;
    let mut failures = 0u32;
    for tier in tiers {
        for tracker in tier {
            for attempt in 0..retry.attempts_per_tracker {
                if cancellation.is_cancelled() {
                    return Err(TrackerError::Transport(TransportError::Cancelled));
                }
                match announce_cancellable(session, tracker, request, limits, cancellation).await {
                    Ok(response) => return Ok((tracker.clone(), response)),
                    Err(error) => {
                        last_error = Some(error);
                        failures = failures.saturating_add(1);
                        if attempt + 1 < retry.attempts_per_tracker {
                            let delay = retry_delay(request.info_hash.0, failures, retry);
                            wait_backoff(delay, cancellation).await?;
                        }
                    }
                }
            }
        }
    }
    Err(last_error.unwrap_or(TrackerError::Protocol))
}

/// Announces an existing native torrent through its stored metainfo or magnet
/// trackers, preserving metainfo tier ordering and checking the request identity.
pub async fn announce_torrent<S: I2pSession + ?Sized>(
    session: &S,
    service: &PersistentTorrentService,
    id: i2pr_tc_core::service::TorrentId,
    request: &AnnounceRequest,
    limits: TrackerLimits,
    retry: RetryPolicy,
    cancellation: &Cancellation,
) -> Result<(String, AnnounceResponse), TrackerError> {
    let snapshot = service.get(id).map_err(|_| TrackerError::Service)?;
    if snapshot.info_hash != request.info_hash.0 {
        return Err(TrackerError::Protocol);
    }
    let tiers = service
        .tracker_tiers(id)
        .map_err(|_| TrackerError::Service)?;
    announce_with_failover(session, &tiers, request, limits, retry, cancellation).await
}

pub fn retry_delay(info_hash: [u8; 20], failure: u32, policy: RetryPolicy) -> Duration {
    let shift = failure.saturating_sub(1).min(31);
    let multiplier = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
    let base = policy
        .initial_delay
        .checked_mul(multiplier)
        .unwrap_or(policy.maximum_delay)
        .min(policy.maximum_delay);
    if policy.jitter.is_zero() {
        return base;
    }
    let seed = u32::from_be_bytes(info_hash[..4].try_into().unwrap()) ^ failure;
    let spread = policy.jitter.as_millis().min(u32::MAX as u128) as u32;
    let extra = if spread == 0 { 0 } else { seed % (spread + 1) };
    base.saturating_add(Duration::from_millis(extra as u64))
        .min(policy.maximum_delay)
}

pub async fn wait_backoff(
    duration: Duration,
    cancellation: &Cancellation,
) -> Result<(), TransportError> {
    let deadline = tokio::time::Instant::now() + duration;
    loop {
        if cancellation.is_cancelled() {
            return Err(TransportError::Cancelled);
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Ok(());
        }
        tokio::time::sleep((deadline - now).min(Duration::from_millis(20))).await;
    }
}

pub(crate) async fn race_cancel<F: Future>(
    future: F,
    cancellation: &Cancellation,
) -> Result<F::Output, TransportError> {
    tokio::pin!(future);
    loop {
        if cancellation.is_cancelled() {
            return Err(TransportError::Cancelled);
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep(Duration::from_millis(20)) => {},
            output = &mut future => return Ok(output),
        }
    }
}

/// Announce identity policy.
///
/// The default omits `User-Agent` entirely. Omission is qualified rather than
/// merely preferred: qBittorrent's Anonymous Mode resets the header to an empty
/// string and keeps announcing successfully against the same trackers, and the
/// I2P BitTorrent specification documents announce parameters without ever
/// mandating the header. A deployed client therefore needs it not at all, so
/// the absence of the header buys the removal of a client fingerprint for free.
///
/// [`TrackerIdentity::Fixed`] exists only as an operator escape hatch for a
/// private tracker that rejects headerless announces. It accepts one stable,
/// non-versioned value and rejects anything version-shaped, so the fingerprint
/// this corrective removed cannot be reintroduced by configuration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TrackerIdentity {
    /// Send no `User-Agent` header.
    #[default]
    Omit,
    /// Send one fixed, non-versioned compatibility value.
    Fixed(&'static str),
}

/// Longest accepted fixed `User-Agent` value.
const MAX_FIXED_USER_AGENT_BYTES: usize = 128;

/// Reject a fixed identity that would reintroduce a version fingerprint.
fn validate_fixed_user_agent(value: &str) -> Result<(), TrackerError> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_FIXED_USER_AGENT_BYTES {
        return Err(TrackerError::Protocol);
    }
    if bytes.iter().any(|byte| !(0x20..=0x7e).contains(byte)) {
        return Err(TrackerError::Protocol);
    }
    // A version fingerprint is any digit run followed by a dotted digit run,
    // such as the `0.1` this corrective removed.
    let mut digits = 0usize;
    for byte in bytes {
        if byte.is_ascii_digit() {
            digits += 1;
        } else if byte == &b'.' && digits > 0 {
            return Err(TrackerError::Protocol);
        } else {
            digits = 0;
        }
    }
    Ok(())
}

pub fn build_request(
    endpoint: &TrackerEndpoint,
    announce: &AnnounceRequest,
) -> Result<Vec<u8>, TrackerError> {
    build_request_with_identity(endpoint, announce, TrackerIdentity::default())
}

pub fn build_request_with_identity(
    endpoint: &TrackerEndpoint,
    announce: &AnnounceRequest,
    identity: TrackerIdentity,
) -> Result<Vec<u8>, TrackerError> {
    if let TrackerIdentity::Fixed(value) = identity {
        validate_fixed_user_agent(value)?;
    }
    identity::validate_i2p_hostname(&endpoint.host).map_err(|_| TrackerError::Protocol)?;
    if !endpoint.path_and_query.starts_with('/')
        || endpoint.path_and_query.contains(['\r', '\n', '#'])
    {
        return Err(TrackerError::Protocol);
    }
    let mut target = endpoint.path_and_query.clone();
    target.push(if target.contains('?') { '&' } else { '?' });
    target.push_str("info_hash=");
    push_percent_encoded(&mut target, &announce.info_hash.0);
    target.push_str("&peer_id=");
    push_percent_encoded(&mut target, &announce.peer_id);
    target.push_str(&format!(
        "&port=0&uploaded={}&downloaded={}&left={}&compact=1&numwant={}",
        announce.uploaded, announce.downloaded, announce.left, announce.numwant
    ));
    if let Some(event) = announce.event {
        target.push_str("&event=");
        target.push_str(event.as_str());
    }
    let host = if endpoint.port == 80 {
        endpoint.host.clone()
    } else {
        format!("{}:{}", endpoint.host, endpoint.port)
    };
    let mut request = format!("GET {target} HTTP/1.1\r\nHost: {host}\r\nAccept: */*\r\n");
    if let TrackerIdentity::Fixed(value) = identity {
        request.push_str("User-Agent: ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("Connection: close\r\n\r\n");
    if request.len() > MAX_REQUEST_BYTES {
        return Err(TrackerError::Limit);
    }
    Ok(request.into_bytes())
}

fn push_percent_encoded(out: &mut String, value: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in value {
        out.push('%');
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 15) as usize] as char);
    }
}

async fn read_http_response<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut R,
    limit: usize,
) -> Result<Vec<u8>, TrackerError> {
    let cap = limit
        .checked_add(MAX_HEADER_BYTES)
        .ok_or(TrackerError::Limit)?;
    let mut response = Vec::with_capacity(cap.min(16 * 1024));
    let mut chunk = [0u8; 8192];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        if response.len().saturating_add(read) > cap {
            return Err(TrackerError::Limit);
        }
        response.extend_from_slice(&chunk[..read]);
    }
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or(TrackerError::Protocol)?;
    if header_end > MAX_HEADER_BYTES {
        return Err(TrackerError::Limit);
    }
    let headers =
        std::str::from_utf8(&response[..header_end]).map_err(|_| TrackerError::Protocol)?;
    let mut lines = headers.split("\r\n");
    let status = lines.next().ok_or(TrackerError::Protocol)?;
    if !(status.starts_with("HTTP/1.0 200 ") || status.starts_with("HTTP/1.1 200 ")) {
        return Err(TrackerError::Protocol);
    }
    let mut content_length = None;
    let mut chunked = false;
    for line in lines {
        let (name, value) = line.split_once(':').ok_or(TrackerError::Protocol)?;
        if name.eq_ignore_ascii_case("content-length") {
            let length = value
                .trim()
                .parse::<usize>()
                .map_err(|_| TrackerError::Protocol)?;
            if length > limit || content_length.replace(length).is_some() {
                return Err(TrackerError::Limit);
            }
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            if !value.trim().eq_ignore_ascii_case("chunked") || chunked {
                return Err(TrackerError::Protocol);
            }
            chunked = true;
        }
    }
    if content_length.is_some() && chunked {
        return Err(TrackerError::Protocol);
    }
    let body = &response[header_end + 4..];
    let decoded = if chunked {
        decode_chunked(body, limit)?
    } else {
        if content_length.is_some_and(|length| length != body.len()) {
            return Err(TrackerError::Protocol);
        }
        body.to_vec()
    };
    if decoded.len() > limit {
        return Err(TrackerError::Limit);
    }
    Ok(decoded)
}

fn decode_chunked(mut input: &[u8], limit: usize) -> Result<Vec<u8>, TrackerError> {
    let mut body = Vec::new();
    loop {
        let line_end = input
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or(TrackerError::Protocol)?;
        let size_text =
            std::str::from_utf8(&input[..line_end]).map_err(|_| TrackerError::Protocol)?;
        if size_text.contains(';') {
            return Err(TrackerError::Protocol);
        }
        let size = usize::from_str_radix(size_text, 16).map_err(|_| TrackerError::Protocol)?;
        input = &input[line_end + 2..];
        if size == 0 {
            if input != b"\r\n" {
                return Err(TrackerError::Protocol);
            }
            break;
        }
        if body.len().saturating_add(size) > limit
            || input.len() < size.saturating_add(2)
            || &input[size..size + 2] != b"\r\n"
        {
            return Err(TrackerError::Limit);
        }
        body.extend_from_slice(&input[..size]);
        input = &input[size + 2..];
    }
    Ok(body)
}

pub fn parse_announce_response(
    input: &[u8],
    peer_limit: usize,
) -> Result<AnnounceResponse, TrackerError> {
    if input.len() > DEFAULT_MAX_RESPONSE_BYTES {
        return Err(TrackerError::Limit);
    }
    let root = bencode::parse(
        input,
        bencode::Limits {
            input: DEFAULT_MAX_RESPONSE_BYTES,
            string: DEFAULT_MAX_RESPONSE_BYTES,
            ..Default::default()
        },
    )
    .map_err(|_| TrackerError::Protocol)?;
    if let Some(reason) = bencode::dict_get(&root, input, b"failure reason") {
        let reason = bencode::bytes(reason, input).ok_or(TrackerError::Protocol)?;
        let reason = String::from_utf8_lossy(reason).chars().take(256).collect();
        return Err(TrackerError::Failure(reason));
    }
    let interval = number(&root, input, b"interval")?.ok_or(TrackerError::Protocol)?;
    if interval <= 0 {
        return Err(TrackerError::Protocol);
    }
    let min_interval = number(&root, input, b"min interval")?
        .filter(|value| *value > 0)
        .map(|seconds| Duration::from_secs(seconds as u64));
    let complete = number(&root, input, b"complete")?.map(to_u32).transpose()?;
    let incomplete = number(&root, input, b"incomplete")?
        .map(to_u32)
        .transpose()?;
    let warning = bencode::dict_get(&root, input, b"warning message")
        .and_then(|value| bencode::bytes(value, input))
        .map(|value| String::from_utf8_lossy(value).chars().take(256).collect());
    let peers = match bencode::dict_get(&root, input, b"peers") {
        None => Vec::new(),
        Some(value) => {
            let bytes = bencode::bytes(value, input).ok_or(TrackerError::Protocol)?;
            if bytes.len() % 32 != 0 || bytes.len() / 32 > peer_limit {
                return Err(TrackerError::Protocol);
            }
            let mut seen = std::collections::BTreeSet::new();
            bytes
                .chunks_exact(32)
                .map(|chunk| <[u8; 32]>::try_from(chunk).expect("fixed chunk"))
                .filter(|hash| seen.insert(*hash))
                .map(I2pPeer::from_hash)
                .collect()
        }
    };
    if bencode::dict_get(&root, input, b"peers6").is_some() {
        return Err(TrackerError::Protocol);
    }
    Ok(AnnounceResponse {
        interval: Duration::from_secs(interval as u64),
        min_interval,
        complete,
        incomplete,
        peers,
        warning,
    })
}

fn number(root: &bencode::Value, input: &[u8], key: &[u8]) -> Result<Option<i64>, TrackerError> {
    match bencode::dict_get(root, input, key) {
        Some(bencode::Value::Integer(number)) => Ok(Some(*number)),
        Some(_) => Err(TrackerError::Protocol),
        None => Ok(None),
    }
}

fn to_u32(value: i64) -> Result<u32, TrackerError> {
    u32::try_from(value).map_err(|_| TrackerError::Protocol)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{identity::Destination, I2pStream};
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    fn compact_response(peers: &[[u8; 32]]) -> Vec<u8> {
        let mut out = b"d8:intervali60e5:peers".to_vec();
        let bytes: Vec<u8> = peers.iter().flat_map(|peer| peer.iter().copied()).collect();
        out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
        out.extend_from_slice(&bytes);
        out.extend_from_slice(b"e");
        out
    }

    #[test]
    fn tracker_urls_fail_closed_outside_i2p_http() {
        for value in [
            "https://tracker.i2p/announce",
            "http://tracker.example/announce",
            "http://127.0.0.1/announce",
            "http://[::1]/announce",
            "http://user@tracker.i2p/announce",
            "http://tracker.i2p/announce\r\nHost: attacker",
        ] {
            assert!(TrackerEndpoint::parse(value).is_err(), "accepted {value:?}");
        }
        assert_eq!(
            TrackerEndpoint::parse("http://tracker.i2p:6969/announce?passkey=x").unwrap(),
            TrackerEndpoint {
                host: "tracker.i2p".into(),
                port: 6969,
                path_and_query: "/announce?passkey=x".into(),
            }
        );
    }

    #[test]
    fn compact_peers_are_hash_only_and_bounded() {
        let peer = [7; 32];
        let response = parse_announce_response(&compact_response(&[peer, peer]), 2).unwrap();
        assert_eq!(response.peers.len(), 1);
        assert_eq!(response.peers[0].hash.0, peer);
        assert!(parse_announce_response(&compact_response(&[peer, [8; 32]]), 1).is_err());
        let malformed = b"d8:intervali60e5:peers6:abcdefghij5:peers6ee";
        assert!(parse_announce_response(malformed, 10).is_err());
    }

    #[test]
    fn retry_backoff_is_deterministic_and_capped() {
        let policy = RetryPolicy {
            attempts_per_tracker: 5,
            initial_delay: Duration::from_secs(2),
            maximum_delay: Duration::from_secs(10),
            jitter: Duration::ZERO,
        };
        assert_eq!(retry_delay([1; 20], 1, policy), Duration::from_secs(2));
        assert_eq!(retry_delay([1; 20], 2, policy), Duration::from_secs(4));
        assert_eq!(retry_delay([1; 20], 20, policy), Duration::from_secs(10));
    }

    #[tokio::test]
    async fn cancellation_interrupts_backoff() {
        let cancellation = Cancellation::default();
        cancellation.cancel();
        assert!(matches!(
            wait_backoff(Duration::from_secs(60), &cancellation).await,
            Err(TransportError::Cancelled)
        ));
    }

    #[derive(Clone)]
    struct FakeSession {
        destination: Destination,
        stream: Arc<Mutex<Option<I2pStream>>>,
        looked_up: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl I2pSession for FakeSession {
        fn local_peer_hash(&self) -> [u8; 32] {
            [9; 32]
        }
        async fn lookup(&self, name: &str) -> Result<Destination, TransportError> {
            self.looked_up.lock().unwrap().push(name.into());
            Ok(self.destination.clone())
        }
        async fn connect(
            &self,
            _destination: &Destination,
            _port: u16,
        ) -> Result<I2pStream, TransportError> {
            self.stream
                .lock()
                .unwrap()
                .take()
                .ok_or(TransportError::Address)
        }
        async fn accept(&self) -> Result<(Destination, I2pStream), TransportError> {
            Err(TransportError::Address)
        }
    }

    #[tokio::test]
    async fn announce_reuses_injected_i2p_session_and_percent_encodes_binary_fields() {
        let (client, mut server) = duplex(64 * 1024);
        let destination = Destination::from_bytes(vec![3; 387]).unwrap();
        let session = FakeSession {
            destination,
            stream: Arc::new(Mutex::new(Some(Box::pin(client)))),
            looked_up: Arc::new(Mutex::new(Vec::new())),
        };
        let body = compact_response(&[[4; 32]]);
        let server_task = tokio::spawn(async move {
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                server.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET /announce?info_hash=%00%00"));
            assert!(request.contains("&peer_id=%05%05"));
            assert!(request.contains(
                "&port=0&uploaded=3&downloaded=2&left=1&compact=1&numwant=1&event=started"
            ));
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            server.write_all(response.as_bytes()).await.unwrap();
            server.write_all(&body).await.unwrap();
        });
        let result = announce(
            &session,
            "http://tracker.i2p/announce",
            &AnnounceRequest {
                info_hash: InfoHashV1([0; 20]),
                peer_id: [5; 20],
                uploaded: 3,
                downloaded: 2,
                left: 1,
                numwant: 1,
                event: Some(AnnounceEvent::Started),
            },
            TrackerLimits::default(),
        )
        .await
        .unwrap();
        server_task.await.unwrap();
        assert_eq!(result.peers[0].hash.0, [4; 32]);
        assert_eq!(&*session.looked_up.lock().unwrap(), &["tracker.i2p"]);
    }

    fn golden_announce() -> AnnounceRequest {
        AnnounceRequest {
            info_hash: InfoHashV1([0xab; 20]),
            peer_id: [
                0x01, 0x02, 0x20, 0x0d, 0x0a, 0x2d, 0x0a, 0x2d, 0x41, 0x42, 0x43, 0x0a, 0x2d, 0x80,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ],
            uploaded: 10,
            downloaded: 20,
            left: 30,
            numwant: 50,
            event: Some(AnnounceEvent::Started),
        }
    }

    #[test]
    fn golden_announce_request_bytes_omit_any_version_fingerprint() {
        let endpoint =
            TrackerEndpoint::parse("http://tracker.i2p/announce?passkey=secret").unwrap();
        let request = build_request(&endpoint, &golden_announce()).unwrap();
        let mut expected = String::from("GET /announce?passkey=secret&info_hash=");
        for byte in [0xabu8; 20] {
            expected.push_str(&format!("%{byte:02X}"));
        }
        expected.push_str("&peer_id=%01%02%20%0D%0A%2D%0A%2D%41%42%43%0A%2D%80%00%00%00%00%00%00");
        expected.push_str(
            "&port=0&uploaded=10&downloaded=20&left=30&compact=1&numwant=50&event=started",
        );
        expected
            .push_str(" HTTP/1.1\r\nHost: tracker.i2p\r\nAccept: */*\r\nConnection: close\r\n\r\n");
        assert_eq!(String::from_utf8(request).unwrap(), expected);
    }

    #[test]
    fn announce_request_never_carries_a_versioned_user_agent() {
        for value in [
            "http://tracker.i2p/announce",
            "http://tracker.i2p:6969/announce",
        ] {
            let endpoint = TrackerEndpoint::parse(value).unwrap();
            let request = String::from_utf8(build_request(&endpoint, &golden_announce()).unwrap())
                .unwrap()
                .to_lowercase();
            assert!(
                !request.contains("user-agent"),
                "{value} leaked a user agent"
            );
            assert!(
                !request.contains("i2pr-tc"),
                "{value} leaked the product name"
            );
            assert!(!request.contains("%2f0.%31"), "{value} leaked a version");
        }
    }

    #[test]
    fn non_default_port_is_announced_in_the_host_header() {
        let endpoint = TrackerEndpoint::parse("http://tracker.i2p:6969/announce").unwrap();
        let request =
            String::from_utf8(build_request(&endpoint, &golden_announce()).unwrap()).unwrap();
        assert!(request.contains("\r\nHost: tracker.i2p:6969\r\n"));
    }

    #[test]
    fn fixed_tracker_identity_accepts_only_a_stable_non_versioned_value() {
        let endpoint = TrackerEndpoint::parse("http://tracker.i2p/announce").unwrap();
        let request = String::from_utf8(
            build_request_with_identity(
                &endpoint,
                &golden_announce(),
                TrackerIdentity::Fixed("bitTorrent"),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(request.contains("\r\nUser-Agent: bitTorrent\r\n"));
        for value in [
            "i2pr-tc/0.1", // boundary-guard:allow
            "client 1.0",
            "qBittorrent 4.6.0",
            "",
            "bad\r\nX: 1",
        ] {
            assert!(
                build_request_with_identity(
                    &endpoint,
                    &golden_announce(),
                    TrackerIdentity::Fixed(value)
                )
                .is_err(),
                "accepted versioned or hostile identity {value:?}"
            );
        }
    }
}
