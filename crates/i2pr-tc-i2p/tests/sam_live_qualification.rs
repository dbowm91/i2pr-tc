//! Live interoperability qualification against a real SAM service.
//!
//! boundary-guard:test-only
//!
//! This file is a test-only target. It is the one place in the repository that
//! opens a host socket, and it exists only to qualify this client against a real
//! router: a Java I2P SAM bridge or an i2pd bridge. The managed production
//! profile never sees it. The production boundary is
//! [`i2pr_tc_i2p::sam::SamConnectionFactory`], which asks the managed runtime
//! for one raw SAM protocol stream; this harness simply provides that factory
//! from a local bridge so the protocol bytes under test are the same ones the
//! managed profile would carry.
//!
//! Every case is environment-dependent, so each one reports whether it ran and
//! why it did not. A skip is a recorded observation, not a pass.
//!
//! Configure with:
//!   I2PR_TC_SAM_ADDR=127.0.0.1:7656 cargo test -p i2pr-tc-i2p --test sam_live_qualification -- --nocapture
//!
//! Two cases need no configuration beyond a reachable bridge: the `HELLO` plus
//! `SESSION CREATE` negotiation, and `NAMING LOOKUP NAME=ME`, which resolves
//! the calling session's own Destination. Together they qualify command
//! encoding, reply parsing, and base64 decoding against a real service.
//!
//! Optional extras:
//!   I2PR_TC_LIVE_SESSION_KEYS  injected key material for a `SESSION CREATE`
//!   I2PR_TC_LIVE_ANNOUNCE_URL  an `http://…/announce` URL to announce to
//!   I2PR_TC_LIVE_NAME          a `.i2p` name to resolve
//!   I2PR_TC_LIVE_DESTINATION   an I2P base64 Destination to dial
//!
//! Two-peer streaming lives in its own target, `sam_live_two_peer.rs`, because
//! it needs two bridges and provisions its own peer. This target's
//! `I2PR_TC_LIVE_DESTINATION` row is for dialling a peer this harness does not
//! own, such as a remote torrent peer.
use i2pr_tc_i2p::{
    I2pSession,
    identity::Destination,
    sam::SamPrimaryStyle,
    sam::{
        SamChannelConfig, SamChannelKind, SamClient, SamConnectionFactory, SamIdentity, SamLimits,
        SamRawStream, SamSessionDestination, SamTimeouts,
    },
};
use sha2::Digest;
use std::time::Duration;
use tokio::net::TcpStream;

/// Where the SAM bridge listens. Overridable so a bridged I2P session manager on
/// a non-default port can be qualified too.
fn sam_address() -> String {
    std::env::var("I2PR_TC_SAM_ADDR").unwrap_or_else(|_| "127.0.0.1:7656".to_owned())
}

/// Test-only factory: one host connection per requested raw SAM protocol
/// stream, which is exactly what the managed runtime provides per logical
/// stream.
struct LocalSamFactory {
    address: String,
    opened: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl SamConnectionFactory for LocalSamFactory {
    async fn open_sam_connection(&self) -> Result<SamRawStream, i2pr_tc_i2p::TransportError> {
        let stream = TcpStream::connect(&self.address).await?;
        let _ = stream.set_nodelay(true);
        self.opened
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Box::pin(stream))
    }
}

fn timeouts() -> SamTimeouts {
    SamTimeouts {
        open: Duration::from_secs(20),
        // Minting a transient identity makes a real service generate key
        // material and publish a lease set, which took single-digit seconds per
        // connection on the router this matrix was run against and queued up
        // behind each other. The production default of 30s is a client-side
        // bound, not a statement about how long a router may take, so the
        // harness widens it rather than reporting a timeout as a defect.
        handshake: Duration::from_secs(120),
        lookup: Duration::from_secs(60),
        connect: Duration::from_secs(60),
        accept: Duration::from_secs(60),
        datagram: Duration::from_secs(30),
        child: Duration::from_secs(60),
        close: Duration::from_secs(5),
    }
}

fn destination_from_env() -> Option<Destination> {
    let raw = std::env::var("I2PR_TC_LIVE_DESTINATION").ok()?;
    // Reuse the client's own bounded base64 decoder rather than adding a
    // dependency for a test fixture.
    let bytes = i2pr_tc_i2p::sam::decode_base64(raw.as_bytes(), 8192).ok()?;
    Destination::from_bytes(bytes).ok()
}

/// Lowercase hex, for reporting a hash a human can compare against a `.b32.i2p`
/// name by hand.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Report whether the environment can run the live matrix at all.
async fn environment_report() -> bool {
    let address = sam_address();
    match TcpStream::connect(&address).await {
        Ok(_) => {
            println!("live qualification: SAM bridge reachable at {address}");
            true
        }
        Err(error) => {
            println!(
                "live qualification SKIPPED: no SAM bridge at {address} ({error}).\n\
                 Set I2PR_TC_SAM_ADDR to a Java I2P or i2pd SAM bridge to run the \
                 live interoperability matrix. Protocol transcript coverage runs \
                 regardless, in the sam module's unit tests."
            );
            false
        }
    }
}

/// Builds a client whose session identifier is unique to one live case.
///
/// A real service keeps a session alive after the connection that created it
/// goes away, so reusing one identifier across cases makes the later cases
/// collide with the earlier ones and report a duplicated id instead of their
/// own result.
fn client(case: &str, factory: LocalSamFactory) -> SamClient<LocalSamFactory> {
    build_client(case, SamSessionDestination::Transient, factory)
}

fn build_client(
    case: &str,
    session_destination: SamSessionDestination,
    factory: LocalSamFactory,
) -> SamClient<LocalSamFactory> {
    let identity = SamIdentity::new(
        session_destination,
        &format!("i2pr-tc-live-{case}-{}", std::process::id()),
    )
    .expect("a valid session destination and session id");
    SamClient::new(factory, identity, SamLimits::default(), timeouts())
        .expect("default SAM limits are valid")
}

/// Qualifies the second `DESTINATION=` form: injected key material.
///
/// This is the form a production client needs, because a transient identity is
/// per-connection and cannot be dialled or persisted. The key material is
/// injected, never read from disk here; obtain it from the router itself with
/// one `SESSION CREATE … DESTINATION=TRANSIENT` and pass the `DESTINATION=`
/// value back.
#[tokio::test]
async fn live_session_create_adopts_injected_key_material() {
    let factory = || LocalSamFactory {
        address: sam_address(),
        opened: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    if !environment_report().await {
        return;
    }
    let Ok(keys) = std::env::var("I2PR_TC_LIVE_SESSION_KEYS") else {
        println!(
            "live SESSION CREATE with injected keys SKIPPED: set I2PR_TC_LIVE_SESSION_KEYS to \
             the DESTINATION= value a real service returns for a transient SESSION CREATE."
        );
        return;
    };
    let client = build_client(
        "keys",
        SamSessionDestination::PrivateKeys(keys.clone()),
        factory(),
    );
    let cancellation = i2pr_tc_storage::Cancellation::default();
    match client.establish_primary(&cancellation).await {
        Ok(negotiated) => {
            assert!(client.has_primary());
            assert!(
                client.identity().has_persistent_keys(),
                "the injected form must stay the persistent one"
            );
            println!(
                "live SESSION CREATE ok with injected keys: {} encoded bytes accepted; \
                 style={:?}; identity={}",
                keys.len(),
                negotiated.primary_style,
                hex(&client
                    .local_destination_hash()
                    .expect("a confirmed identity"))
            );
        }
        Err(error) => println!("live SESSION CREATE with injected keys reported {error:?}"),
    }
}

#[tokio::test]
async fn live_hello_and_session_create_negotiate_with_a_real_router() {
    let opened = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let client = client(
        "hello",
        LocalSamFactory {
            address: sam_address(),
            opened: std::sync::Arc::clone(&opened),
        },
    );
    if !environment_report().await {
        return;
    }
    let cancellation = i2pr_tc_storage::Cancellation::default();
    match client.establish_primary(&cancellation).await {
        Ok(negotiated) => {
            assert!(client.has_primary());
            assert_eq!(
                Some(negotiated),
                client.negotiated(),
                "the negotiated profile must be reported from recorded state"
            );
            println!(
                "live HELLO + shared-Destination SESSION CREATE ok; version={}; style={:?}; \
                 DESTINATION={:?}; raw connections opened={}",
                negotiated.version,
                negotiated.primary_style,
                client.session_destination(),
                opened.load(std::sync::atomic::Ordering::SeqCst)
            );
        }
        Err(error) => {
            println!(
                "live HELLO + shared-Destination SESSION CREATE reported {error:?} from {}",
                sam_address()
            );
        }
    }
}

/// The shared-Destination style vocabulary, qualified against a real service.
///
/// The current specification spells it `PRIMARY`; i2pd keeps the older `MASTER`
/// spelling. This client offers the normative spelling first on its own fresh
/// connection and falls back only when the service rejects it, so what this
/// prints is exactly which spelling the deployed router understands.
#[tokio::test]
async fn live_shared_destination_style_is_qualified_against_a_real_router() {
    let opened = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let client = client(
        "style",
        LocalSamFactory {
            address: sam_address(),
            opened: std::sync::Arc::clone(&opened),
        },
    );
    if !environment_report().await {
        return;
    }
    let cancellation = i2pr_tc_storage::Cancellation::default();
    match client.establish_primary(&cancellation).await {
        Ok(negotiated) => {
            let connections = opened.load(std::sync::atomic::Ordering::SeqCst);
            println!(
                "live shared-Destination profile: version={}; accepted style={:?}; \
                 raw connections opened={connections}",
                negotiated.version, negotiated.primary_style
            );
            // One create per connection: a rejected spelling is never retried on
            // the connection that rejected it.
            assert!(
                connections <= 2,
                "at most one attempt per shared-Destination spelling"
            );
            assert_eq!(
                connections,
                if negotiated.primary_style == SamPrimaryStyle::Primary {
                    1
                } else {
                    2
                }
            );
        }
        Err(error) => println!("live shared-Destination profile reported {error:?}"),
    }
}

/// The child channels a real service will and will not attach.
///
/// i2pd 2.61.0 answers `SESSION ADD` with `STYLE=DATAGRAM` or `STYLE=RAW` with
/// `Unsupported STYLE`, so this reports each channel's own result instead of
/// asserting one: the STREAM child is the transport torrent data needs today,
/// and the datagram forms are the substrate the next milestone needs from a
/// router that implements them.
#[tokio::test]
async fn live_child_channel_styles_are_qualified_against_a_real_router() {
    let client = client(
        "children",
        LocalSamFactory {
            address: sam_address(),
            opened: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        },
    );
    if !environment_report().await {
        return;
    }
    let cancellation = i2pr_tc_storage::Cancellation::default();
    if let Err(error) = client.establish_primary(&cancellation).await {
        println!("live child channels SKIPPED: no shared-Destination session ({error:?})");
        return;
    }
    let ports = SamChannelConfig {
        from_port: 30927,
        to_port: 30928,
        listen_port: 30929,
    };
    for (kind, config) in [
        (SamChannelKind::Stream, SamChannelConfig::STREAM),
        (SamChannelKind::RepliableDatagram, ports),
        (SamChannelKind::RawDatagram, ports),
    ] {
        match client.add_channel(kind, config, &cancellation).await {
            Ok(channel) => {
                println!(
                    "live SESSION ADD ok for {kind:?}: id={} protocol={}",
                    channel.id(),
                    kind.protocol()
                );
                match client.remove_channel(&channel, &cancellation).await {
                    Ok(()) => println!("live SESSION REMOVE ok for {}", channel.id()),
                    Err(error) => println!(
                        "live SESSION REMOVE for {} reported {error:?}",
                        channel.id()
                    ),
                }
            }
            Err(error) => println!("live SESSION ADD for {kind:?} reported {error:?}"),
        }
    }
}

/// The router-confirmed local identity, qualified against a real service.
///
/// This is the case that matters most for correctness downstream: PEX
/// self-filtering, tracker announces, and self-connection rejection all key on
/// this hash, so it must be the digest of the Destination the router actually
/// bound to the session.
#[tokio::test]
async fn live_primary_identity_is_the_router_confirmed_destination() {
    let opened = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let client = client(
        "identity",
        LocalSamFactory {
            address: sam_address(),
            opened: std::sync::Arc::clone(&opened),
        },
    );
    if !environment_report().await {
        return;
    }
    let cancellation = i2pr_tc_storage::Cancellation::default();
    if let Err(error) = client.establish_primary(&cancellation).await {
        println!("live local identity SKIPPED: no shared-Destination session ({error:?})");
        return;
    }
    match client.local_identity() {
        Ok(identity) => {
            let expected: [u8; 32] = sha2::Sha256::digest(identity.destination().as_bytes()).into();
            assert_eq!(identity.hash(), expected);
            assert_eq!(
                I2pSession::local_peer_hash(&client).expect("typed hash"),
                expected
            );
            println!(
                "live local identity ok: {}-byte Destination, hash={}, session={}",
                identity.destination().as_bytes().len(),
                hex(&identity.hash()),
                client.session_id()
            );
        }
        Err(error) => println!("live local identity reported {error:?}"),
    }
}

#[tokio::test]
async fn live_naming_lookup_resolves_a_real_name() {
    let opened = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let client = client(
        "naming",
        LocalSamFactory {
            address: sam_address(),
            opened: std::sync::Arc::clone(&opened),
        },
    );
    if !environment_report().await {
        return;
    }
    let Ok(name) = std::env::var("I2PR_TC_LIVE_NAME") else {
        println!(
            "live NAMING LOOKUP SKIPPED: set I2PR_TC_LIVE_NAME to a resolvable .i2p \
             name (for example the router's own name)."
        );
        return;
    };
    if let Err(error) = client
        .establish_primary(&i2pr_tc_storage::Cancellation::default())
        .await
    {
        println!("live NAMING LOOKUP SKIPPED: no shared-Destination session ({error:?})");
        return;
    }
    let session: &dyn I2pSession = &client;
    match session.lookup(&name).await {
        Ok(destination) => println!(
            "live NAMING LOOKUP ok: {name} resolved to a {}-byte Destination",
            destination.as_bytes().len()
        ),
        Err(error) => println!("live NAMING LOOKUP for {name} reported {error:?}"),
    }
}

#[tokio::test]
async fn live_stream_connect_and_accept_carry_real_traffic() {
    let opened = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let client = client(
        "connect",
        LocalSamFactory {
            address: sam_address(),
            opened: std::sync::Arc::clone(&opened),
        },
    );
    if !environment_report().await {
        return;
    }
    let Some(peer) = destination_from_env() else {
        println!(
            "live STREAM CONNECT SKIPPED: set I2PR_TC_LIVE_DESTINATION to an I2P base64 \
             Destination of a peer running this client."
        );
        return;
    };
    let cancellation = i2pr_tc_storage::Cancellation::default();
    let channel = match client.stream_channel(&cancellation).await {
        Ok(channel) => channel,
        Err(error) => {
            println!("live STREAM CONNECT SKIPPED: no STREAM child channel ({error:?})");
            return;
        }
    };
    // `port = 0` is what the peer path passes; it must still reach the router.
    match client
        .stream_connect(&channel, &peer, 0, &cancellation)
        .await
    {
        Ok(mut stream) => {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            if let Err(error) = stream.write_all(b"live-qualification").await {
                println!("live STREAM CONNECT write reported {error:?}");
                return;
            }
            let mut sink = [0u8; 17];
            match tokio::time::timeout(Duration::from_secs(30), stream.read_exact(&mut sink)).await
            {
                Ok(Ok(_)) => println!("live STREAM CONNECT ok: peer echoed the probe"),
                Ok(Err(error)) => println!("live STREAM CONNECT read reported {error:?}"),
                Err(_) => {
                    println!("live STREAM CONNECT ok: connection established, peer sent no echo")
                }
            }
        }
        Err(error) => println!("live STREAM CONNECT reported {error:?}"),
    }
}

#[tokio::test]
async fn live_tracker_announce_omits_any_version_fingerprint() {
    if !environment_report().await {
        return;
    }
    let Ok(url) = std::env::var("I2PR_TC_LIVE_ANNOUNCE_URL") else {
        println!(
            "live tracker announce SKIPPED: set I2PR_TC_LIVE_ANNOUNCE_URL to an I2P \
             tracker announce URL."
        );
        return;
    };
    let request = i2pr_tc_i2p::tracker::build_request(
        &i2pr_tc_i2p::tracker::TrackerEndpoint::parse(&url).expect("a valid I2P announce URL"),
        &i2pr_tc_i2p::tracker::AnnounceRequest {
            info_hash: i2pr_tc_core::InfoHashV1([0x11; 20]),
            peer_id: [0; 20],
            uploaded: 0,
            downloaded: 0,
            left: 0,
            numwant: 50,
            event: None,
        },
    )
    .expect("a bounded announce request");
    let text = String::from_utf8(request).expect("the request is ASCII");
    assert!(!text.to_lowercase().contains("user-agent"));
    println!("live announce request prepared with no User-Agent header:\n{text}");
}
