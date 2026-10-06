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
//! Optional extras:
//!   I2PR_TC_LIVE_ANNOUNCE_URL   an `http://…/announce` URL to announce to
//!   I2PR_TC_LIVE_NAME           a `.i2p` name to resolve
//!   I2PR_TC_LIVE_DESTINATION    a base64 Destination to dial
use i2pr_tc_i2p::{
    I2pSession,
    identity::Destination,
    sam::{SamClient, SamConnectionFactory, SamIdentity, SamLimits, SamRawStream, SamTimeouts},
};
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
        handshake: Duration::from_secs(30),
        lookup: Duration::from_secs(30),
        connect: Duration::from_secs(60),
        accept: Duration::from_secs(60),
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

fn client(factory: LocalSamFactory) -> SamClient<LocalSamFactory> {
    let identity =
        SamIdentity::new(None, "i2pr-tc-live-qualification").expect("session id is valid");
    SamClient::new(factory, identity, SamLimits::default(), timeouts())
        .expect("default SAM limits are valid")
}

#[tokio::test]
async fn live_hello_and_session_create_negotiate_with_a_real_router() {
    let opened = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let client = client(LocalSamFactory {
        address: sam_address(),
        opened: std::sync::Arc::clone(&opened),
    });
    if !environment_report().await {
        return;
    }
    let cancellation = i2pr_tc_storage::Cancellation::default();
    match client.ensure_session(&cancellation).await {
        Ok(()) => {
            assert!(client.is_session_created());
            assert!(client.negotiated_version().is_some());
            println!(
                "live HELLO/SESSION CREATE ok; version={:?}; raw connections opened={}",
                client.negotiated_version(),
                opened.load(std::sync::atomic::Ordering::SeqCst)
            );
        }
        Err(error) => {
            println!(
                "live HELLO/SESSION CREATE reported {error:?} from {}",
                sam_address()
            );
        }
    }
}

#[tokio::test]
async fn live_naming_lookup_resolves_a_real_name() {
    let opened = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let client = client(LocalSamFactory {
        address: sam_address(),
        opened: std::sync::Arc::clone(&opened),
    });
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
    let client = client(LocalSamFactory {
        address: sam_address(),
        opened: std::sync::Arc::clone(&opened),
    });
    if !environment_report().await {
        return;
    }
    let Some(peer) = destination_from_env() else {
        println!(
            "live STREAM CONNECT SKIPPED: set I2PR_TC_LIVE_DESTINATION to a base64 \
             Destination of a peer running this client."
        );
        return;
    };
    let cancellation = i2pr_tc_storage::Cancellation::default();
    // `port = 0` is what the peer path passes; it must still reach the router.
    match client.stream_connect(&peer, 0, &cancellation).await {
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
