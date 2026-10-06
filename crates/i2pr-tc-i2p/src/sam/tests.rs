//! Unit qualification for the shared-Destination SAM client.
//!
//! The harness below is a scripted service rather than a mock: each connection
//! advances through expected command octets and answers with the reply that a
//! real bridge sends for that command. That makes the assertions about exact
//! wire order and about what the client *never* sends — a second
//! `SESSION CREATE`, a `DESTINATION=` on `SESSION ADD`, a host forwarding
//! option — real properties of this client rather than of the harness.
use super::*;
use crate::identity;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};

/// Builds one structurally valid Destination for fixtures.
fn synthetic_destination(seed: u8) -> identity::Destination {
    identity::Destination::from_bytes(synthetic_destination_bytes(seed))
        .expect("fixture destination is well formed")
}

fn synthetic_destination_bytes(seed: u8) -> Vec<u8> {
    // The real layout: 256-byte public key, 128-byte signing key, then a
    // three-byte null certificate (type 0, zero-length payload).
    let mut bytes: Vec<u8> = (0..384u32)
        .map(|index| (index as u8).wrapping_add(seed))
        .collect();
    bytes.extend([0u8, 0u8, 0u8]);
    bytes
}

/// A Destination with a non-empty certificate, which the two length bytes at
/// 385-386 must account for exactly.
fn synthetic_destination_with_certificate(seed: u8) -> Vec<u8> {
    let mut bytes = synthetic_destination_bytes(seed);
    bytes[384] = 5;
    bytes[385] = 0;
    bytes[386] = 4;
    bytes.extend([1, 2, 3, 4]);
    bytes
}

fn private_key_blob() -> String {
    // A Destination followed by a private key and a signing private key, which
    // is what a real service returns in the `DESTINATION=` option of a
    // transient `SESSION CREATE` reply (663 or more bytes).
    let mut bytes = synthetic_destination_bytes(9);
    bytes.extend([7u8; 276]);
    encode_base64(&bytes)
}

#[derive(Clone)]
struct Step {
    expect: Vec<u8>,
    reply: Vec<u8>,
    then_close: bool,
}

impl Step {
    fn new(expect: &str, reply: &str) -> Self {
        Self {
            expect: expect.as_bytes().to_vec(),
            reply: reply.as_bytes().to_vec(),
            then_close: false,
        }
    }

    fn closing(expect: &str, reply: &str) -> Self {
        Self {
            then_close: true,
            ..Self::new(expect, reply)
        }
    }
}

/// One scripted connection: `[expect, reply]` pairs, in order.
type Script = Vec<Step>;

const HELLO_33: &str = "HELLO REPLY RESULT=OK VERSION=3.3\n";
/// Raw peer bytes a connected stream carries after its status line.
const PEER_BYTES: &[u8] = b"peer-bytes";
const OK: &str = "SESSION STATUS RESULT=OK\n";

/// One connection's captured octets.
type ConnectionLog = Arc<Mutex<Vec<u8>>>;

/// Writes every connection's octets into a shared log so a test can assert on
/// what the client sent, per connection.
struct Recorder {
    read: ReadHalf<tokio::io::DuplexStream>,
    write: WriteHalf<tokio::io::DuplexStream>,
    log: Arc<Mutex<Vec<u8>>>,
}

impl tokio::io::AsyncRead for Recorder {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.read).poll_read(context, buffer)
    }
}

impl tokio::io::AsyncWrite for Recorder {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let log = std::pin::Pin::new(&mut self.log);
        if let Ok(mut log) = log.lock() {
            log.extend_from_slice(buffer);
        }
        std::pin::Pin::new(&mut self.write).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.write).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.write).poll_shutdown(context)
    }
}

struct ScriptedFactory {
    scripts: Vec<Mutex<Script>>,
    index: Mutex<usize>,
    logs: Arc<Mutex<Vec<ConnectionLog>>>,
    opened: Arc<AtomicUsize>,
    open_failure: Option<TransportError>,
}

impl Clone for ScriptedFactory {
    fn clone(&self) -> Self {
        Self {
            scripts: self
                .scripts
                .iter()
                .map(|steps| Mutex::new(steps.lock().expect("script lock").clone()))
                .collect(),
            index: Mutex::new(*self.index.lock().expect("index lock")),
            logs: Arc::clone(&self.logs),
            opened: Arc::clone(&self.opened),
            open_failure: self
                .open_failure
                .as_ref()
                .map(|error| TransportError::Session(error.to_string())),
        }
    }
}

impl Default for ScriptedFactory {
    fn default() -> Self {
        Self {
            scripts: Vec::new(),
            index: Mutex::new(0),
            logs: Arc::new(Mutex::new(Vec::new())),
            opened: Arc::new(AtomicUsize::new(0)),
            open_failure: None,
        }
    }
}

impl ScriptedFactory {
    fn new(scripts: Vec<Script>) -> Self {
        Self {
            scripts: scripts.into_iter().map(Mutex::new).collect(),
            ..Self::default()
        }
    }

    /// Every octet written on the connection at `index`.
    fn written(&self, index: usize) -> Vec<u8> {
        self.logs
            .lock()
            .expect("log lock")
            .get(index)
            .map(|log| log.lock().expect("connection log").clone())
            .unwrap_or_default()
    }

    fn count(&self, needle: &str) -> usize {
        String::from_utf8_lossy(&self.joined())
            .matches(needle)
            .count()
    }

    fn joined(&self) -> Vec<u8> {
        let logs = self.logs.lock().expect("log lock");
        let mut joined = Vec::new();
        for log in logs.iter() {
            joined.extend_from_slice(&log.lock().expect("connection log"));
        }
        joined
    }
}

#[async_trait]
impl SamConnectionFactory for ScriptedFactory {
    async fn open_sam_connection(&self) -> Result<SamRawStream, TransportError> {
        if let Some(error) = &self.open_failure {
            return Err(io::Error::other(error.to_string()).into());
        }
        let index = {
            let mut index = self.index.lock().expect("index lock");
            let current = *index;
            *index += 1;
            current
        };
        self.opened.fetch_add(1, Ordering::SeqCst);
        let steps = self
            .scripts
            .get(index)
            .map(|steps| steps.lock().expect("script lock").clone())
            .unwrap_or_default();
        let (client, service) = tokio::io::duplex(64 * 1024);
        let log = Arc::new(Mutex::new(Vec::new()));
        self.logs.lock().expect("log lock").push(Arc::clone(&log));
        tokio::spawn(serve(service, steps));
        let (read, write) = tokio::io::split(client);
        Ok(Box::pin(Recorder { read, write, log }))
    }
}

/// Advances one connection through its script, answering each expected command
/// only once that command's octets have actually arrived.
async fn serve(mut service: tokio::io::DuplexStream, mut steps: Script) {
    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        if !steps.is_empty() {
            let step = steps.first().expect("a step is observed");
            if step.expect.is_empty() {
                let close = step.then_close;
                if service.write_all(&step.reply).await.is_err() {
                    return;
                }
                if close {
                    return;
                }
                steps.remove(0);
                continue;
            }
            // A command is answered only once every one of its octets has
            // arrived, and the consumed prefix is dropped so the next command
            // is matched against what follows it.
            let expected = &step.expect;
            let common = buffer.len().min(expected.len());
            if buffer[..common] == expected[..common] {
                if buffer.len() >= expected.len() {
                    let close = step.then_close;
                    buffer.drain(..expected.len());
                    if service.write_all(&step.reply).await.is_err() {
                        return;
                    }
                    if close {
                        return;
                    }
                    steps.remove(0);
                    continue;
                }
            } else {
                // Not a command this script expects; a real service would ignore
                // it, so drop it and keep waiting.
                buffer.clear();
            }
        }
        match service.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(count) => buffer.extend_from_slice(&chunk[..count]),
        }
    }
}

fn identity_for(name: &str) -> SamIdentity {
    SamIdentity::new(SamSessionDestination::Transient, &format!("{name}-sid"))
        .expect("fixture identity is valid")
}

fn test_timeouts() -> SamTimeouts {
    SamTimeouts {
        open: Duration::from_millis(200),
        handshake: Duration::from_millis(200),
        lookup: Duration::from_millis(200),
        connect: Duration::from_millis(200),
        accept: Duration::from_millis(200),
        datagram: Duration::from_millis(200),
        child: Duration::from_millis(200),
        close: Duration::from_millis(200),
    }
}

fn build(factory: &ScriptedFactory, name: &str) -> SamClient<ScriptedFactory> {
    SamClient::new(
        factory.clone(),
        identity_for(name),
        SamLimits::default(),
        test_timeouts(),
    )
    .expect("client configuration is valid")
}

/// The control-connection script for a primary that is created successfully.
fn primary_script(destination: &identity::Destination) -> Script {
    vec![
        Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
        Step::new(
            &format!(
                "SESSION CREATE STYLE=PRIMARY ID={} DESTINATION=TRANSIENT\n",
                "i2pr-sid"
            ),
            &format!(
                "SESSION STATUS RESULT=OK DESTINATION={}\n",
                encode_base64(destination.as_bytes())
            ),
        ),
    ]
}

fn destination_line(destination: &identity::Destination) -> String {
    format!("{}\n", encode_base64(destination.as_bytes()))
}

fn assert_hello_only(written: &[u8], name: &str) {
    let text = String::from_utf8_lossy(written);
    assert!(
        text.starts_with("HELLO VERSION MIN=3.1 MAX=3.3\n"),
        "{name} must negotiate with HELLO first, wrote {text:?}"
    );
    assert!(
        !text.contains("SESSION CREATE"),
        "{name} must never create a session: {text:?}"
    );
}

#[test]
fn sam_commands_are_exact_octets_for_the_shared_destination_profile() {
    let destination = SamSessionDestination::Transient;
    assert_eq!(
        encode_hello(SamVersion::new(3, 1), SamVersion::new(3, 3)).unwrap(),
        b"HELLO VERSION MIN=3.1 MAX=3.3\n"
    );
    assert_eq!(
        encode_session_create_primary("sid", &destination, SamPrimaryStyle::Primary).unwrap(),
        b"SESSION CREATE STYLE=PRIMARY ID=sid DESTINATION=TRANSIENT\n"
    );
    assert_eq!(
        encode_session_create_primary("sid", &destination, SamPrimaryStyle::Master).unwrap(),
        b"SESSION CREATE STYLE=MASTER ID=sid DESTINATION=TRANSIENT\n"
    );

    // A child never names a destination and never asks the router to forward to
    // a host it names; a stream child names no ports at all.
    for kind in [
        SamChannelKind::RepliableDatagram,
        SamChannelKind::RawDatagram,
    ] {
        let add = String::from_utf8(
            encode_session_add(
                kind,
                "sid",
                SamChannelConfig {
                    from_port: 1,
                    to_port: 2,
                    listen_port: 3,
                },
            )
            .unwrap(),
        )
        .expect("ASCII");
        assert!(!add.contains("HOST"), "{add}");
        assert!(!add.contains("sam.udp"), "{add}");
        assert!(!add.contains("DESTINATION"), "{add}");
    }
    assert_eq!(
        encode_session_add(
            SamChannelKind::Stream,
            "sid-stream-1",
            SamChannelConfig::STREAM
        )
        .unwrap(),
        b"SESSION ADD STYLE=STREAM ID=sid-stream-1\n"
    );
    assert_eq!(
        encode_session_add(
            SamChannelKind::RepliableDatagram,
            "sid-d-1",
            SamChannelConfig {
                from_port: 12345,
                to_port: 53,
                listen_port: 12346,
            },
        )
        .unwrap(),
        b"SESSION ADD STYLE=DATAGRAM ID=sid-d-1 PORT=12345 FROM_PORT=12345 TO_PORT=53 LISTEN_PORT=12346\n"
    );
    assert_eq!(
        encode_session_add(
            SamChannelKind::RawDatagram,
            "sid-r-1",
            SamChannelConfig {
                from_port: 12345,
                to_port: 69,
                listen_port: 12347,
            },
        )
        .unwrap(),
        b"SESSION ADD STYLE=RAW ID=sid-r-1 PORT=12345 FROM_PORT=12345 TO_PORT=69 LISTEN_PORT=12347 PROTOCOL=18\n"
    );
    assert_eq!(
        encode_session_remove("sid-stream-1").unwrap(),
        b"SESSION REMOVE ID=sid-stream-1\n"
    );

    let peer = synthetic_destination(4);
    assert_eq!(
        encode_stream_connect("sid-stream-1", peer.as_bytes(), 7071).unwrap(),
        format!(
            "STREAM CONNECT ID=sid-stream-1 DESTINATION={} PORT=7071 SILENT=false\n",
            encode_base64(peer.as_bytes())
        )
        .into_bytes()
    );
    assert_eq!(
        encode_stream_accept("sid-stream-1").unwrap(),
        b"STREAM ACCEPT ID=sid-stream-1 SILENT=false\n"
    );
    assert_eq!(
        encode_datagram_receive("sid-d-1").unwrap(),
        b"DATAGRAM RECEIVE ID=sid-d-1\n"
    );
    assert_eq!(
        encode_raw_receive("sid-r-1").unwrap(),
        b"RAW DATA RECEIVE ID=sid-r-1\n"
    );

    // Datagram framing is one command line plus exactly SIZE bytes of payload.
    let send = encode_datagram_send(
        "sid-d-1",
        peer.as_bytes(),
        SamChannelConfig {
            from_port: 12345,
            to_port: 53,
            listen_port: 12346,
        },
        b"ping",
    )
    .unwrap();
    let split = send
        .iter()
        .position(|byte| *byte == b'\n')
        .expect("datagram send has a command line");
    assert_eq!(
        &send[..split],
        format!(
            "DATAGRAM SEND ID=sid-d-1 DESTINATION={} FROM_PORT=12345 TO_PORT=53 SIZE=4",
            encode_base64(peer.as_bytes())
        )
        .as_bytes()
    );
    assert_eq!(&send[split + 1..], b"ping");

    let raw = encode_raw_send(
        "sid-r-1",
        peer.as_bytes(),
        SamChannelConfig {
            from_port: 12345,
            to_port: 69,
            listen_port: 12347,
        },
        b"\x00\x01",
    )
    .unwrap();
    let split = raw
        .iter()
        .position(|byte| *byte == b'\n')
        .expect("raw send has a command line");
    assert_eq!(
        &raw[..split],
        format!(
            "RAW DATA SEND ID=sid-r-1 DESTINATION={} FROM_PORT=12345 TO_PORT=69 PROTOCOL=18 SIZE=2",
            encode_base64(peer.as_bytes())
        )
        .as_bytes()
    );
    assert_eq!(&raw[split + 1..], b"\x00\x01");
}

#[test]
fn sam_encoders_reject_unrepresentable_input_before_any_io() {
    for token in ["", "a b", "a=b", "a\"b", &"x".repeat(600)] {
        assert!(
            encode_session_remove(token).is_err(),
            "{token:?} must not be sent"
        );
    }
    // Key material is checked as a value, so base64 padding is legal there.
    assert!(
        encode_session_create_primary(
            "sid",
            &SamSessionDestination::PrivateKeys(private_key_blob()),
            SamPrimaryStyle::Primary,
        )
        .is_ok()
    );
    for keys in ["", "not base64!", "a\"b"] {
        assert!(
            SamIdentity::new(SamSessionDestination::PrivateKeys(keys.to_owned()), "sid").is_err(),
            "{keys:?} must not become a session destination"
        );
    }
    // Datagram payloads are bounded before a connection is even opened.
    assert!(
        encode_datagram_send(
            "sid-d-1",
            &[0u8; 387],
            SamChannelConfig::STREAM,
            &vec![0u8; MAX_DATAGRAM_PAYLOAD_BYTES + 1],
        )
        .is_err()
    );
    assert!(encode_datagram_send("sid-d-1", &[0u8; 387], SamChannelConfig::STREAM, b"",).is_err());
}

#[test]
fn sam_base64_is_i2p_padded_and_rejects_bad_input() {
    // Every index 62 and 63 value uses I2P's substitution alphabet.
    let encoded = encode_base64(&[0xff, 0xff, 0xfe]);
    assert!(encoded.contains('-') && encoded.contains('~'));
    assert!(!encoded.contains('+') && !encoded.contains('/'));
    assert_eq!(
        decode_base64(encoded.as_bytes(), 64).unwrap(),
        vec![0xff, 0xff, 0xfe]
    );
    assert_eq!(encode_base64(b"a"), "YQ==");
    assert_eq!(decode_base64(b"YQ==", 8).unwrap(), b"a".to_vec());

    for bad in [
        &b"YQ="[..], // not quad aligned
        b"YQ=A",     // misplaced padding
        b"====",     // over-padded
        b"YQ==YQ==", // padding in the middle
        b"YQ+A",     // standard alphabet symbol
        b"YQ/A",
        b"YWJ\x01", // non-alphabet byte
        b"YQ=\"\n",
    ] {
        assert!(decode_base64(bad, 64).is_err(), "{bad:?} must be rejected");
    }
    // A value larger than the bound is a limit violation, not a decode attempt.
    assert!(matches!(
        decode_base64(&[b'A'; 4096], 16),
        Err(SamError::Limit)
    ));
}

#[test]
fn sam_reply_parsing_accepts_every_status_line_shape() {
    let limits = SamLimits::default();
    let reply = parse_reply_block(b"HELLO REPLY RESULT=OK VERSION=3.3\n", &limits)
        .expect("hello reply parses");
    assert_eq!(reply.kind(), SamReplyKind::Hello);
    assert_eq!(reply.result(), Some(SamResult::Ok));
    assert_eq!(reply.option("VERSION"), Some("3.3"));

    // A datagram reply is a status line plus option lines; the `SIZE` payload
    // that follows it is raw data and is read as exactly that many bytes.
    let datagram = parse_reply_block(
        b"DATAGRAM RECEIVED DESTINATION=abc SIZE=4 FROM_PORT=1 TO_PORT=2\n",
        &limits,
    )
    .expect("datagram reply parses");
    assert_eq!(datagram.kind(), SamReplyKind::Datagram);
    assert_eq!(datagram.number::<u16>("SIZE").unwrap(), 4);
    assert_eq!(datagram.number_or("FROM_PORT", 0u16).unwrap(), 1);

    let raw =
        parse_reply_block(b"RAW DATA SEND RESULT=OK\n", &limits).expect("raw send reply parses");
    assert_eq!(raw.kind(), SamReplyKind::Raw);
    assert_eq!(raw.result(), Some(SamResult::Ok));

    // A status line plus following option lines is one reply block.
    let naming = parse_reply_block(b"NAMING REPLY RESULT=OK NAME=x\nVALUE=abc\n", &limits)
        .expect("naming reply parses");
    assert_eq!(naming.kind(), SamReplyKind::Naming);
    assert_eq!(naming.option("VALUE"), Some("abc"));
    assert_eq!(naming.option("DESTINATION"), None);

    // A quoted value may carry spaces; an unquoted one may not.
    let message = parse_reply_block(
        b"SESSION STATUS RESULT=I2P_ERROR MESSAGE=\"Unknown STYLE\"\n",
        &limits,
    )
    .expect("quoted message parses");
    assert_eq!(message.result(), Some(SamResult::I2pError));
    assert_eq!(message.option("MESSAGE"), Some("Unknown STYLE"));
}

#[test]
fn sam_reply_parsing_bounds_options_and_rejects_duplicates() {
    let strict = SamLimits::strict();
    let long_value = format!("SESSION STATUS RESULT=OK DESTINATION={}\n", "A".repeat(64));
    assert!(matches!(
        parse_reply_block(long_value.as_bytes(), &strict),
        Err(SamError::Limit)
    ));
    assert!(matches!(
        parse_reply_block(
            b"SESSION STATUS RESULT=OK\nVERSION=3.3\nVERSION=3.3\n",
            &SamLimits::default()
        ),
        Err(SamError::Protocol(_))
    ));
    // One reply block is one command's reply: a second status line is not an
    // option line and must not be swallowed.
    assert!(
        parse_reply_block(
            b"SESSION STATUS RESULT=OK\nSESSION STATUS RESULT=OK\n",
            &SamLimits::default()
        )
        .is_err()
    );
    assert!(matches!(
        parse_reply_block(&vec![b'A'; 64 * 1024], &SamLimits::default()),
        Err(SamError::Limit)
    ));
}

#[test]
fn sam_reply_parsing_rejects_malformed_lines() {
    let limits = SamLimits::default();
    for bad in [
        &b""[..],
        b"NONSENSE REPLY RESULT=OK\n",
        b"SESSION STATUS\n",
        b"SESSION STATUS RESULT=NOPE\n",
        b"SESSION STATUS RESULT=OK VERSION=3.3 =9\n",
        b"SESSION STATUS RESULT=OK VERSION=3.3 EXTRA\n",
        b"SESSION STATUS RESULT=OK\nVERSION=3.3\n \n",
        b"\xff\xfe\n",
    ] {
        assert!(
            parse_reply_block(bad, &limits).is_err(),
            "{:?} must be rejected",
            String::from_utf8_lossy(bad)
        );
    }
    // Numeric options are range checked where the client reads them.
    let reply = parse_reply_block(b"DATAGRAM RECEIVED SIZE=70000\n", &limits).unwrap();
    assert!(reply.number::<u16>("SIZE").is_err());
}

#[test]
fn sam_destination_parsing_distinguishes_a_destination_from_a_key_blob() {
    let destination = synthetic_destination(3);
    let encoded = encode_base64(destination.as_bytes());
    assert_eq!(decode_destination(&encoded).unwrap(), destination);
    // A non-null certificate is accepted, and its declared length must match
    // the value's actual length exactly.
    let certified = synthetic_destination_with_certificate(4);
    assert!(is_destination_structure(&certified));
    assert_eq!(
        decode_destination(&encode_base64(&certified)).unwrap(),
        identity::Destination::from_bytes(certified.clone()).unwrap()
    );
    let mut lying = certified.clone();
    lying[386] = 9;
    assert!(!is_destination_structure(&lying));
    assert!(decode_destination(&encode_base64(&lying)).is_err());

    // Private key material starts with a Destination, so only a structural
    // check can tell the two apart; a length range cannot. This is the exact
    // shape a real service returns for a transient session.
    let blob = decode_base64(private_key_blob().as_bytes(), MAX_DESTINATION_BYTES).unwrap();
    assert_eq!(blob.len(), 663);
    assert!(is_destination_structure(&blob[..387]));
    assert!(!is_destination_structure(&blob));
    assert!(decode_destination(private_key_blob().as_str()).is_err());
    assert!(is_destination_structure(destination.as_bytes()));
    assert!(decode_destination("not base64!").is_err());
    assert!(decode_destination(&encode_base64(&[7u8; 900])).is_err());
}

/// The layout a real router sends, captured verbatim from i2pd 2.61.0.
///
/// This is the regression that a certificate-first reading of a Destination
/// would miss: a keys-first value whose first byte is ordinary key material
/// must still be recognised as a Destination.
#[test]
fn sam_accepts_the_keys_first_destination_layout_a_real_router_sends() {
    let destination = synthetic_destination(3);
    let bytes = destination.as_bytes();
    assert_eq!(bytes.len(), 387);
    // Keys first, then the certificate. A certificate-first reading would
    // parse the first byte as a length prefix and then require the remainder
    // to be exactly 384 bytes, which this layout does not satisfy.
    // A certificate-first reading would require the value to be a short
    // prefix followed by exactly 384 more bytes, which this layout is not.
    assert_ne!(bytes.len(), 3 + bytes[0] as usize + 384);
    assert!(is_destination_structure(bytes));
    assert_eq!(
        decode_destination(&encode_base64(bytes)).unwrap(),
        destination
    );
}

#[tokio::test]
async fn sam_identity_is_unavailable_until_the_router_confirms_it() {
    let factory = ScriptedFactory::default();
    let client = build(&factory, "i2pr");
    assert!(matches!(
        client.local_identity(),
        Err(TransportError::IdentityNotReady)
    ));
    assert!(matches!(
        client.local_destination_hash(),
        Err(TransportError::IdentityNotReady)
    ));
    assert!(matches!(
        I2pSession::local_peer_hash(&client),
        Err(TransportError::IdentityNotReady)
    ));
    assert!(!client.has_primary());
    assert_eq!(client.generation(), 0);
}

#[tokio::test]
async fn sam_primary_is_created_once_per_generation_on_one_connection() {
    let destination = synthetic_destination(1);
    let factory = ScriptedFactory::new(vec![primary_script(&destination)]);
    let client = build(&factory, "i2pr");
    let negotiated = client
        .establish_primary(&Cancellation::default())
        .await
        .expect("primary is created");
    assert_eq!(negotiated.version, SamVersion::new(3, 3));
    assert_eq!(negotiated.primary_style, SamPrimaryStyle::Primary);
    // A second call must not mint a second session.
    let again = client
        .establish_primary(&Cancellation::default())
        .await
        .expect("primary already exists");
    assert_eq!(again, negotiated);
    assert_eq!(client.local_identity().unwrap().hash(), destination.hash());
    assert_eq!(factory.count("SESSION CREATE"), 1);
    assert_eq!(factory.opened.load(Ordering::SeqCst), 1);
    assert_eq!(client.generation(), 1);
}

#[tokio::test]
async fn sam_primary_falls_back_to_the_legacy_style_on_its_own_connection() {
    let destination = synthetic_destination(2);
    let create_primary = "SESSION CREATE STYLE=PRIMARY ID=i2pr-sid DESTINATION=TRANSIENT\n";
    let create_master = "SESSION CREATE STYLE=MASTER ID=i2pr-sid DESTINATION=TRANSIENT\n";
    let factory = ScriptedFactory::new(vec![
        vec![
            Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
            Step::new(
                create_primary,
                "SESSION STATUS RESULT=I2P_ERROR MESSAGE=\"Unknown STYLE\"\n",
            ),
            // A rejected style must not leave a second command on the same
            // connection: the fallback gets its own.
        ],
        vec![
            Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
            Step::new(
                create_master,
                &format!(
                    "SESSION STATUS RESULT=OK DESTINATION={}\n",
                    encode_base64(destination.as_bytes())
                ),
            ),
        ],
    ]);
    let client = build(&factory, "i2pr");
    let negotiated = client
        .establish_primary(&Cancellation::default())
        .await
        .expect("the legacy spelling is accepted");
    assert_eq!(negotiated.primary_style, SamPrimaryStyle::Master);
    assert_eq!(factory.opened.load(Ordering::SeqCst), 2);
    // One create per connection, never two on one connection.
    assert_eq!(factory.count("SESSION CREATE"), 2);
    assert_eq!(factory.count(create_primary), 1);
    assert_eq!(factory.count(create_master), 1);
    assert_eq!(client.generation(), 1);
}

#[tokio::test]
async fn sam_primary_rejects_a_service_without_the_shared_profile() {
    let factory = ScriptedFactory::new(vec![vec![Step::new(
        "HELLO VERSION MIN=3.1 MAX=3.3\n",
        "HELLO REPLY RESULT=OK VERSION=3.2\n",
    )]]);
    let client = build(&factory, "i2pr");
    let error = client
        .establish_primary(&Cancellation::default())
        .await
        .expect_err("3.2 has no shared-Destination session");
    assert!(matches!(error, TransportError::Session(_)), "{error:?}");
    assert_eq!(factory.count("SESSION CREATE"), 0);
    assert!(!client.has_primary());
}

#[tokio::test]
async fn sam_hello_rejects_no_version_and_unsupported_versions() {
    for (reply, expected) in [
        (
            "HELLO REPLY RESULT=NOVERSION\n",
            TransportError::SamReply(SamResult::NoVersion),
        ),
        (
            "HELLO REPLY RESULT=OK VERSION=4.0\n",
            TransportError::Session("sam advertised unsupported version 4.0".to_owned()),
        ),
        (
            "HELLO REPLY RESULT=OK VERSION=nonsense\n",
            TransportError::Protocol,
        ),
        (
            "HELLO REPLY RESULT=I2P_ERROR MESSAGE=\"nope\"\n",
            TransportError::SamReply(SamResult::I2pError),
        ),
    ] {
        let factory = ScriptedFactory::new(vec![vec![Step::new(
            "HELLO VERSION MIN=3.1 MAX=3.3\n",
            reply,
        )]]);
        let client = build(&factory, "i2pr");
        let error = client
            .establish_primary(&Cancellation::default())
            .await
            .expect_err("an unusable version must fail");
        assert!(
            error.to_string() == expected.to_string(),
            "{reply}: got {error:?}"
        );
        assert_eq!(factory.count("SESSION CREATE"), 0, "{reply}");
    }
}

#[tokio::test]
async fn sam_local_identity_falls_back_to_naming_me() {
    let destination = synthetic_destination(5);
    // A transient session may be answered with private key material in the
    // create reply. That is not a Destination and must never become the local
    // identity, so the confirmation path is used instead.
    let factory = ScriptedFactory::new(vec![vec![
        Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
        Step::new(
            "SESSION CREATE STYLE=PRIMARY ID=i2pr-sid DESTINATION=TRANSIENT\n",
            &format!(
                "SESSION STATUS RESULT=OK DESTINATION={}\n",
                private_key_blob()
            ),
        ),
        Step::new(
            "NAMING LOOKUP NAME=ME\n",
            &format!(
                "NAMING REPLY RESULT=OK NAME=ME\nVALUE={}\n",
                encode_base64(destination.as_bytes())
            ),
        ),
    ]]);
    let client = build(&factory, "i2pr");
    client
        .establish_primary(&Cancellation::default())
        .await
        .expect("primary is created");
    assert_eq!(client.local_identity().unwrap().hash(), destination.hash());
    assert_eq!(client.local_identity().unwrap().destination(), &destination);
}

#[tokio::test]
async fn sam_child_channels_are_attached_on_the_control_connection() {
    let destination = synthetic_destination(6);
    let mut control = primary_script(&destination);
    control.push(Step::new(
        "SESSION ADD STYLE=STREAM ID=i2pr-sid-stream-1\n",
        OK,
    ));
    control.push(Step::new(
        "SESSION ADD STYLE=DATAGRAM ID=i2pr-sid-datagram-1 PORT=1 FROM_PORT=1 TO_PORT=2 LISTEN_PORT=3\n",
        OK,
    ));
    control.push(Step::new("SESSION REMOVE ID=i2pr-sid-stream-1\n", OK));
    let factory = ScriptedFactory::new(vec![control]);
    let client = build(&factory, "i2pr");
    client
        .establish_primary(&Cancellation::default())
        .await
        .expect("primary is created");
    let stream = client
        .add_channel(
            SamChannelKind::Stream,
            SamChannelConfig::STREAM,
            &Cancellation::default(),
        )
        .await
        .expect("stream child is attached");
    assert_eq!(stream.generation(), client.generation());
    assert_eq!(client.channel_count(), 1);
    let datagram = client
        .add_channel(
            SamChannelKind::RepliableDatagram,
            SamChannelConfig {
                from_port: 1,
                to_port: 2,
                listen_port: 3,
            },
            &Cancellation::default(),
        )
        .await
        .expect("datagram child is attached");
    assert_eq!(datagram.kind(), SamChannelKind::RepliableDatagram);
    assert_eq!(client.channel_count(), 2);
    client
        .remove_channel(&stream, &Cancellation::default())
        .await
        .expect("child is removed");
    assert_eq!(client.channel_count(), 1);
    // Every attach and detach ran on the control connection, and no child
    // connection opened a session of its own.
    let written = String::from_utf8_lossy(&factory.written(0)).to_string();
    assert!(written.contains("SESSION ADD STYLE=STREAM"));
    assert!(written.contains("SESSION REMOVE"));
    assert_eq!(factory.opened.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sam_stream_connect_and_accept_attach_without_creating_a_session() {
    let destination = synthetic_destination(7);
    let peer = synthetic_destination(8);
    let mut control = primary_script(&destination);
    control.push(Step::new(
        "SESSION ADD STYLE=STREAM ID=i2pr-sid-stream-1\n",
        OK,
    ));
    let connect = format!(
        "STREAM CONNECT ID=i2pr-sid-stream-1 DESTINATION={} PORT=7071 SILENT=false\n",
        encode_base64(peer.as_bytes())
    );
    let factory = ScriptedFactory::new(vec![
        control,
        vec![
            Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
            // Everything after a connect status line is already raw peer data.
            Step::new(
                &connect,
                &format!(
                    "STREAM STATUS RESULT=OK DESTINATION={}\n{}",
                    encode_base64(destination.as_bytes()),
                    String::from_utf8_lossy(PEER_BYTES)
                ),
            ),
        ],
        vec![
            Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
            // The connecting peer's Destination is the line after the status.
            Step::new(
                "STREAM ACCEPT ID=i2pr-sid-stream-1 SILENT=false\n",
                &format!("STREAM STATUS RESULT=OK\n{}", destination_line(&peer)),
            ),
        ],
    ]);
    let client = build(&factory, "i2pr");
    client
        .establish_primary(&Cancellation::default())
        .await
        .unwrap();
    let channel = client
        .add_channel(
            SamChannelKind::Stream,
            SamChannelConfig::STREAM,
            &Cancellation::default(),
        )
        .await
        .expect("stream child is attached");
    let mut outbound = client
        .stream_connect(&channel, &peer, 7071, &Cancellation::default())
        .await
        .expect("connect attaches to the existing session");
    let mut raw = vec![0u8; PEER_BYTES.len()];
    tokio::time::timeout(Duration::from_secs(1), outbound.read_exact(&mut raw))
        .await
        .expect("peer data arrives")
        .expect("read succeeds");
    assert_eq!(&raw, PEER_BYTES);

    let (accepted, _inbound) = client
        .stream_accept(&channel, &Cancellation::default())
        .await
        .expect("accept attaches to the existing session");
    assert_eq!(accepted, peer);

    // Neither data connection created a session, and both negotiated with
    // HELLO only.
    assert_hello_only(&factory.written(1), "connect connection");
    assert_hello_only(&factory.written(2), "accept connection");
    assert_eq!(factory.count("SESSION CREATE"), 1);
    assert_eq!(factory.count("SESSION ADD"), 1);
}

#[tokio::test]
async fn sam_datagram_and_raw_children_frame_on_the_bridge_socket() {
    let destination = synthetic_destination(10);
    let peer = synthetic_destination(11);
    let mut control = primary_script(&destination);
    control.push(Step::new(
        "SESSION ADD STYLE=DATAGRAM ID=i2pr-sid-datagram-1 PORT=7 FROM_PORT=7 TO_PORT=8 LISTEN_PORT=7\n",
        OK,
    ));
    control.push(Step::new(
        "SESSION ADD STYLE=RAW ID=i2pr-sid-raw-1 PORT=9 FROM_PORT=9 TO_PORT=10 LISTEN_PORT=9 PROTOCOL=18\n",
        OK,
    ));
    let datagram_send = format!(
        "DATAGRAM SEND ID=i2pr-sid-datagram-1 DESTINATION={} FROM_PORT=7 TO_PORT=8 SIZE=4\n",
        encode_base64(peer.as_bytes())
    );
    let datagram_reply = format!(
        "DATAGRAM RECEIVED DESTINATION={} SIZE=5 FROM_PORT=7 TO_PORT=8\nhello",
        encode_base64(peer.as_bytes())
    );
    let raw_send = format!(
        "RAW DATA SEND ID=i2pr-sid-raw-1 DESTINATION={} FROM_PORT=9 TO_PORT=10 PROTOCOL=18 SIZE=3\n",
        encode_base64(peer.as_bytes())
    );
    let raw_reply = "RAW DATA RECEIVED RESULT=OK SIZE=3 PROTOCOL=18 FROM_PORT=9 TO_PORT=10\nabc";
    let factory = ScriptedFactory::new(vec![
        control,
        vec![
            Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
            Step::new(&datagram_send, "DATAGRAM SEND RESULT=OK\n"),
        ],
        vec![
            Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
            Step::new("DATAGRAM RECEIVE ID=i2pr-sid-datagram-1\n", &datagram_reply),
        ],
        vec![
            Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
            Step::new(&raw_send, "RAW DATA SEND RESULT=OK\n"),
        ],
        vec![
            Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
            Step::new("RAW DATA RECEIVE ID=i2pr-sid-raw-1\n", raw_reply),
        ],
    ]);
    let client = build(&factory, "i2pr");
    client
        .establish_primary(&Cancellation::default())
        .await
        .unwrap();
    let ports = SamChannelConfig {
        from_port: 7,
        to_port: 8,
        listen_port: 7,
    };
    let datagram_channel = client
        .add_channel(
            SamChannelKind::RepliableDatagram,
            ports,
            &Cancellation::default(),
        )
        .await
        .expect("datagram child is attached");
    client
        .datagram_send(
            &datagram_channel,
            &peer,
            ports,
            b"ping",
            &Cancellation::default(),
        )
        .await
        .expect("datagram is sent");
    let received = client
        .datagram_receive(&datagram_channel, &Cancellation::default())
        .await
        .expect("datagram is received");
    assert_eq!(received.payload, b"hello");
    assert_eq!(received.from_port, 7);
    assert_eq!(received.to_port, 8);
    assert_eq!(received.sender.as_ref(), Some(&peer));

    let raw_channel = client
        .add_channel(
            SamChannelKind::RawDatagram,
            SamChannelConfig {
                from_port: 9,
                to_port: 10,
                listen_port: 9,
            },
            &Cancellation::default(),
        )
        .await
        .expect("raw child is attached");
    client
        .raw_send(
            &raw_channel,
            &peer,
            SamChannelConfig {
                from_port: 9,
                to_port: 10,
                listen_port: 9,
            },
            b"xyz",
            &Cancellation::default(),
        )
        .await
        .expect("raw datagram is sent");
    let raw_received = client
        .raw_receive(&raw_channel, &Cancellation::default())
        .await
        .expect("raw datagram is received");
    assert_eq!(raw_received.payload, b"abc");
    assert_eq!(raw_received.protocol, I2P_PROTOCOL_RAW);

    // Every datagram exchange ran on its own connection with no session create.
    for index in 1..5 {
        assert_hello_only(&factory.written(index), "datagram connection");
    }
    assert_eq!(factory.count("SESSION CREATE"), 1);
}

#[tokio::test]
async fn sam_datagram_payload_bounds_are_enforced_without_io() {
    let destination = synthetic_destination(12);
    let mut control = primary_script(&destination);
    control.push(Step::new(
        "SESSION ADD STYLE=DATAGRAM ID=i2pr-sid-datagram-1 PORT=1 FROM_PORT=1 TO_PORT=2 LISTEN_PORT=1\n",
        OK,
    ));
    let factory = ScriptedFactory::new(vec![control]);
    let client = build(&factory, "i2pr");
    client
        .establish_primary(&Cancellation::default())
        .await
        .unwrap();
    let ports = SamChannelConfig {
        from_port: 1,
        to_port: 2,
        listen_port: 1,
    };
    let channel = client
        .add_channel(
            SamChannelKind::RepliableDatagram,
            ports,
            &Cancellation::default(),
        )
        .await
        .expect("datagram child is attached");
    let opened = factory.opened.load(Ordering::SeqCst);
    let error = client
        .datagram_send(
            &channel,
            &destination,
            ports,
            &vec![0u8; MAX_DATAGRAM_PAYLOAD_BYTES + 1],
            &Cancellation::default(),
        )
        .await
        .expect_err("an oversized datagram is refused");
    assert!(matches!(error, TransportError::Protocol));
    assert_eq!(factory.opened.load(Ordering::SeqCst), opened);
}

#[tokio::test]
async fn sam_primary_loss_makes_every_child_of_that_generation_stale() {
    let destination = synthetic_destination(13);
    let mut control = primary_script(&destination);
    // The router accepts the child and then drops the control connection.
    control.push(Step::closing(
        "SESSION ADD STYLE=STREAM ID=i2pr-sid-stream-1\n",
        OK,
    ));
    let factory = ScriptedFactory::new(vec![control]);
    let client = build(&factory, "i2pr");
    client
        .establish_primary(&Cancellation::default())
        .await
        .unwrap();
    let channel = client
        .add_channel(
            SamChannelKind::Stream,
            SamChannelConfig::STREAM,
            &Cancellation::default(),
        )
        .await
        .expect("child is attached");
    let generation = client.generation();
    let cancellation = Cancellation::default();
    let error = client
        .add_channel(
            SamChannelKind::RawDatagram,
            SamChannelConfig::STREAM,
            &cancellation,
        )
        .await
        .expect_err("a lost primary cannot take new children");
    assert!(
        matches!(
            error,
            TransportError::Io(_) | TransportError::Stale(_) | TransportError::Session(_)
        ),
        "unexpected error: {error:?}"
    );
    // The identity is a property of the primary, so it goes with it.
    assert!(matches!(
        client.local_identity(),
        Err(TransportError::IdentityNotReady)
    ));
    assert_ne!(client.generation(), generation);
    // A child of the lost generation can never attach to anything again.
    assert!(matches!(
        client
            .remove_channel(&channel, &Cancellation::default())
            .await,
        Err(TransportError::Stale(_))
    ));
}

#[tokio::test]
async fn sam_stale_child_cannot_attach_to_a_replacement_identity() {
    let first = synthetic_destination(14);
    let second = synthetic_destination(15);
    let peer = synthetic_destination(16);
    let mut first_control = primary_script(&first);
    first_control.push(Step::closing(
        "SESSION ADD STYLE=STREAM ID=i2pr-sid-stream-1\n",
        OK,
    ));
    let mut second_control = primary_script(&second);
    second_control.push(Step::new(
        "SESSION ADD STYLE=STREAM ID=i2pr-sid-stream-2\n",
        OK,
    ));
    let factory = ScriptedFactory::new(vec![
        first_control,
        second_control,
        vec![
            Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
            Step::new(
                &format!(
                    "STREAM CONNECT ID=i2pr-sid-stream-2 DESTINATION={} PORT=7071 SILENT=false\n",
                    encode_base64(peer.as_bytes())
                ),
                "STREAM STATUS RESULT=OK\n",
            ),
        ],
    ]);
    let client = build(&factory, "i2pr");
    client
        .establish_primary(&Cancellation::default())
        .await
        .expect("first primary");
    let stale = client
        .add_channel(
            SamChannelKind::Stream,
            SamChannelConfig::STREAM,
            &Cancellation::default(),
        )
        .await
        .expect("first generation child");
    assert_eq!(client.local_identity().unwrap().hash(), first.hash());
    let first_generation = client.generation();

    // Loss of the control connection is observed on the next control command,
    // and the transport reports it rather than reconnecting on its own.
    assert!(
        client
            .add_channel(
                SamChannelKind::RawDatagram,
                SamChannelConfig::STREAM,
                &Cancellation::default(),
            )
            .await
            .is_err()
    );
    assert!(matches!(
        client.local_identity(),
        Err(TransportError::IdentityNotReady)
    ));

    // Restoration is explicit, and the new primary is a new identity.
    client
        .establish_primary(&Cancellation::default())
        .await
        .expect("second primary");
    assert_eq!(client.local_identity().unwrap().hash(), second.hash());
    assert!(client.generation() > first_generation);
    let fresh = client
        .add_channel(
            SamChannelKind::Stream,
            SamChannelConfig::STREAM,
            &Cancellation::default(),
        )
        .await
        .expect("second generation child");
    assert_eq!(fresh.generation(), client.generation());
    client
        .stream_connect(&fresh, &peer, 7071, &Cancellation::default())
        .await
        .expect("the new generation is usable");

    // The child of the lost generation fails instead of attaching to the new
    // identity, which is the whole point of the model.
    assert!(matches!(
        client
            .remove_channel(&stale, &Cancellation::default())
            .await,
        Err(TransportError::Stale(_))
    ));
    assert!(matches!(
        client
            .stream_connect(&stale, &peer, 7071, &Cancellation::default())
            .await,
        Err(TransportError::Stale(_))
    ));
    assert_eq!(factory.count("SESSION CREATE"), 2);
}

#[tokio::test]
async fn sam_reply_timeout_and_eof_are_bounded_failures() {
    // A service that never answers must not hang a caller.
    let factory = ScriptedFactory::new(vec![vec![Step::new(
        "HELLO VERSION MIN=3.1 MAX=3.3\n",
        HELLO_33,
    )]]);
    let client = build(&factory, "i2pr");
    assert!(matches!(
        client.establish_primary(&Cancellation::default()).await,
        Err(TransportError::Timeout)
    ));
    assert_eq!(client.in_flight(), 0);
    assert!(!client.has_primary());

    // A service that closes without answering reports I/O failure.
    let factory = ScriptedFactory::new(vec![vec![Step {
        expect: Vec::new(),
        reply: b"HELLO REPLY RESULT=OK VERSION=3.3\n".to_vec(),
        then_close: false,
    }]]);
    let client = build(&factory, "i2pr");
    assert!(
        client
            .establish_primary(&Cancellation::default())
            .await
            .is_err()
    );
    assert_eq!(client.in_flight(), 0);
}

#[tokio::test]
async fn sam_pending_operations_are_cancellable() {
    let destination = synthetic_destination(17);
    let mut control = primary_script(&destination);
    // The accept status never arrives, so the caller must be able to walk away.
    control.push(Step::new(
        "SESSION ADD STYLE=STREAM ID=i2pr-sid-stream-1\n",
        OK,
    ));
    let factory = ScriptedFactory::new(vec![
        control,
        vec![Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33)],
    ]);
    let client = build(&factory, "i2pr");
    client
        .establish_primary(&Cancellation::default())
        .await
        .expect("primary is created");
    let channel = client
        .add_channel(
            SamChannelKind::Stream,
            SamChannelConfig::STREAM,
            &Cancellation::default(),
        )
        .await
        .expect("child is attached");
    let cancellation = Cancellation::default();
    let canceller = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        canceller.cancel();
    });
    let Err(error) = client.stream_accept(&channel, &cancellation).await else {
        panic!("a blocked accept must not complete on its own");
    };
    assert!(matches!(error, TransportError::Cancelled), "{error:?}");
    assert_eq!(client.in_flight(), 0);
}

#[tokio::test]
async fn sam_close_cancels_in_flight_work_and_fails_dependent_operations() {
    let destination = synthetic_destination(18);
    let factory = ScriptedFactory::new(vec![primary_script(&destination)]);
    let client = build(&factory, "i2pr");
    client
        .establish_primary(&Cancellation::default())
        .await
        .expect("primary is created");
    client.close().await.expect("close drains in-flight work");
    assert!(!client.is_available());
    assert!(client.is_shutdown());
    assert!(!client.has_primary());
    assert!(matches!(
        client.local_identity(),
        Err(TransportError::IdentityNotReady)
    ));
    assert!(matches!(
        client.establish_primary(&Cancellation::default()).await,
        Err(TransportError::Cancelled)
    ));
}

#[tokio::test]
async fn sam_naming_lookup_resolves_from_a_fresh_connection() {
    let peer = synthetic_destination(19);
    let factory = ScriptedFactory::new(vec![vec![
        Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
        Step::new(
            "NAMING LOOKUP NAME=peer.b32.i2p\n",
            &format!(
                "NAMING REPLY RESULT=OK NAME=peer.b32.i2p\nVALUE={}\n",
                encode_base64(peer.as_bytes())
            ),
        ),
    ]]);
    let client = build(&factory, "i2pr");
    let resolved = client
        .nam_lookup("peer.b32.i2p", &Cancellation::default())
        .await
        .expect("naming resolves");
    assert_eq!(resolved, peer);
    assert_hello_only(&factory.written(0), "naming connection");
    // A failed lookup is typed, not a hang and not a silent zero hash.
    let factory = ScriptedFactory::new(vec![vec![
        Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
        Step::new(
            "NAMING LOOKUP NAME=nope.b32.i2p\n",
            "NAMING REPLY RESULT=KEY_NOT_FOUND\n",
        ),
    ]]);
    let client = build(&factory, "i2pr");
    assert!(matches!(
        client
            .nam_lookup("nope.b32.i2p", &Cancellation::default())
            .await,
        Err(TransportError::SamReply(SamResult::KeyNotFound))
    ));
}

#[tokio::test]
async fn sam_connect_failures_carry_the_router_result() {
    let destination = synthetic_destination(20);
    let peer = synthetic_destination(21);
    let mut control = primary_script(&destination);
    control.push(Step::new(
        "SESSION ADD STYLE=STREAM ID=i2pr-sid-stream-1\n",
        OK,
    ));
    let factory = ScriptedFactory::new(vec![
        control,
        vec![
            Step::new("HELLO VERSION MIN=3.1 MAX=3.3\n", HELLO_33),
            Step::new(
                &format!(
                    "STREAM CONNECT ID=i2pr-sid-stream-1 DESTINATION={} PORT=1 SILENT=false\n",
                    encode_base64(peer.as_bytes())
                ),
                "STREAM STATUS RESULT=CANT_REACH_PEER\n",
            ),
        ],
    ]);
    let client = build(&factory, "i2pr");
    client
        .establish_primary(&Cancellation::default())
        .await
        .expect("primary is created");
    let channel = client
        .add_channel(
            SamChannelKind::Stream,
            SamChannelConfig::STREAM,
            &Cancellation::default(),
        )
        .await
        .expect("child is attached");
    assert!(matches!(
        client
            .stream_connect(&channel, &peer, 1, &Cancellation::default())
            .await,
        Err(TransportError::SamReply(SamResult::CantReachPeer))
    ));
}

#[tokio::test]
async fn sam_control_queue_applies_explicit_backpressure() {
    let destination = synthetic_destination(22);
    let mut control = primary_script(&destination);
    control.push(Step::new(
        "SESSION ADD STYLE=STREAM ID=i2pr-sid-stream-1\n",
        OK,
    ));
    // The router accepts exactly one attach and then stops answering, so the
    // primary connection is the bottleneck and callers must be told.
    let factory = ScriptedFactory::new(vec![control]);
    let client = Arc::new(build(&factory, "i2pr"));
    client
        .establish_primary(&Cancellation::default())
        .await
        .expect("primary is created");
    let mut attaches = Vec::new();
    for _ in 0..PRIMARY_QUEUE_DEPTH + 4 {
        let client = Arc::clone(&client);
        attaches.push(tokio::spawn(async move {
            client
                .add_channel(
                    SamChannelKind::Stream,
                    SamChannelConfig::STREAM,
                    &Cancellation::default(),
                )
                .await
        }));
    }
    let mut backpressure = 0;
    for attach in attaches {
        if matches!(
            attach.await.expect("task joins"),
            Err(TransportError::Session(_))
        ) {
            backpressure += 1;
        }
    }
    assert!(
        backpressure > 0,
        "a saturated control queue must report backpressure"
    );
}

#[test]
fn sam_limits_and_timeouts_reject_unusable_configuration() {
    assert!(SamLimits::strict().validate().is_ok());
    assert!(SamLimits::default().validate().is_ok());
    let unusable = SamLimits {
        max_line_bytes: 4,
        ..SamLimits::default()
    };
    assert!(unusable.validate().is_err());
    let zero = SamTimeouts {
        open: Duration::ZERO,
        ..test_timeouts()
    };
    assert!(zero.validate().is_err());
    assert!(SamVersion::parse("3.3").is_ok());
    assert!(SamVersion::parse("3.33.3").is_err());
    assert!(SamVersion::parse("3").is_err());
    assert!(SamVersion::parse("3.x").is_err());
}
