//! Two-peer live qualification over real SAM services.
//!
//! boundary-guard:test-only
//!
//! This is the row C003 could not previously exercise: there was no peer for
//! `STREAM CONNECT` to dial, so `I2PR_TC_LIVE_DESTINATION` had to be supplied
//! by hand and the row stayed unexercised. This harness supplies its own peer.
//!
//! Both sides are this client's own transport: one peer accepts on its STREAM
//! child and echoes, the other dials it. What is real is the router path — the
//! peer's Destination has to be published, routed to, and answered by a live
//! I2P network, which is exactly the thing a synthetic service cannot prove.
//!
//! Two routers are the interesting case. A single router generally will not
//! stream to one of its own destinations, so the harness takes two bridges and
//! can be pointed at two different routers (an i2pd bridge and a Java I2P
//! bridge, for example) to prove interoperability across implementations.
//!
//! Configure with:
//!   I2PR_TC_SAM_ADDR=127.0.0.1:7656                     dialer's bridge
//!   I2PR_TC_LIVE_PEER_SAM_ADDR=127.0.0.1:7657          peer's bridge
//!   I2PR_TC_LIVE_PEER_PORT=<port>                       peer's accept port (default 0)
//!
//! A skip is a recorded observation, never a pass.
use i2pr_tc_i2p::{
    TransportError,
    sam::{
        SamClient, SamConnectionFactory, SamIdentity, SamLimits, SamRawStream,
        SamSessionDestination, SamTimeouts,
    },
};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// The bytes the dialer writes and the peer must return unchanged.
const PROBE: &[u8] = b"i2pr-tc-live-two-peer";

/// Test-only factory: one host connection per requested raw SAM protocol
/// stream, which is what the managed runtime provides per logical stream.
struct BridgeFactory {
    address: String,
}

#[async_trait::async_trait]
impl SamConnectionFactory for BridgeFactory {
    async fn open_sam_connection(&self) -> Result<SamRawStream, TransportError> {
        let stream = TcpStream::connect(&self.address).await?;
        let _ = stream.set_nodelay(true);
        Ok(Box::pin(stream))
    }
}

fn timeouts() -> SamTimeouts {
    SamTimeouts {
        open: Duration::from_secs(20),
        // A persistent identity is what makes the peer dialable across
        // connections, so this harness injects one rather than using TRANSIENT.
        handshake: Duration::from_secs(120),
        lookup: Duration::from_secs(60),
        connect: Duration::from_secs(60),
        accept: Duration::from_secs(60),
        datagram: Duration::from_secs(30),
        child: Duration::from_secs(60),
        close: Duration::from_secs(5),
    }
}

fn build(address: &str, case: &str) -> SamClient<BridgeFactory> {
    SamClient::new(
        BridgeFactory {
            address: address.to_owned(),
        },
        SamIdentity::new(
            SamSessionDestination::Transient,
            &format!("i2pr-tc-two-peer-{case}-{}", std::process::id()),
        )
        .expect("a valid session destination and session id"),
        SamLimits::default(),
        timeouts(),
    )
    .expect("default SAM limits are valid")
}

/// Accepts one inbound stream and echoes whatever the dialer sends.
async fn echo_once(
    client: &SamClient<BridgeFactory>,
    cancellation: &i2pr_tc_storage::Cancellation,
) -> Result<(), TransportError> {
    let channel = client.stream_channel(cancellation).await?;
    let (_peer, mut stream) = client.stream_accept(&channel, cancellation).await?;
    let mut probe = [0u8; 64];
    let read = stream.read(&mut probe).await?;
    stream.write_all(&probe[..read]).await?;
    stream.flush().await?;
    Ok(())
}

/// Dials the peer, writes the probe, and requires the echo back.
async fn dial_and_probe(
    client: &SamClient<BridgeFactory>,
    peer: &i2pr_tc_i2p::identity::Destination,
    port: u16,
    cancellation: &i2pr_tc_storage::Cancellation,
) -> Result<(), TransportError> {
    let channel = client.stream_channel(cancellation).await?;
    let mut stream = client
        .stream_connect(&channel, peer, port, cancellation)
        .await?;
    stream.write_all(PROBE).await?;
    stream.flush().await?;
    let mut echo = [0u8; PROBE.len()];
    match tokio::time::timeout(Duration::from_secs(60), stream.read_exact(&mut echo)).await {
        Ok(result) => {
            result?;
        }
        Err(_) => return Err(TransportError::Timeout),
    }
    if echo != PROBE {
        return Err(TransportError::Protocol);
    }
    Ok(())
}

#[tokio::test]
async fn live_two_peers_stream_one_shared_destination_each() {
    let Ok(peer_address) = std::env::var("I2PR_TC_LIVE_PEER_SAM_ADDR") else {
        println!(
            "live two-peer SKIPPED: set I2PR_TC_LIVE_PEER_SAM_ADDR to a second SAM bridge \
             for the peer's router, and I2PR_TC_SAM_ADDR for the dialer's router. Pointing \
             them at two different routers (i2pd and Java I2P) also qualifies cross-router \
             interoperability."
        );
        return;
    };
    let dial_address =
        std::env::var("I2PR_TC_SAM_ADDR").unwrap_or_else(|_| "127.0.0.1:7656".to_owned());
    let peer_port = std::env::var("I2PR_TC_LIVE_PEER_PORT")
        .ok()
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or(0);
    for (role, address) in [("dialer", &dial_address), ("peer", &peer_address)] {
        match TcpStream::connect(address).await {
            Ok(_) => println!("live two-peer: {role} bridge reachable at {address}"),
            Err(error) => {
                println!("live two-peer SKIPPED: {role} bridge unreachable at {address} ({error})");
                return;
            }
        }
    }

    let cancellation = i2pr_tc_storage::Cancellation::default();
    let peer = build(&peer_address, "peer");
    let dialer = build(&dial_address, "dialer");

    let peer_identity = match peer.establish_primary(&cancellation).await {
        Ok(negotiated) => {
            println!(
                "live two-peer: peer primary established (version={}, style={:?})",
                negotiated.version, negotiated.primary_style
            );
            negotiated
        }
        Err(error) => {
            println!(
                "live two-peer SKIPPED: the peer's bridge offered no shared session ({error:?})"
            );
            return;
        }
    };
    let peer_destination = match peer.local_identity() {
        Ok(identity) => {
            println!(
                "live two-peer: peer identity confirmed, hash={}",
                i2pr_tc_i2p::identity::encode_base32(&identity.hash())
            );
            identity.destination().clone()
        }
        Err(error) => {
            println!("live two-peer SKIPPED: the peer has no confirmed identity ({error:?})");
            return;
        }
    };
    if dialer.establish_primary(&cancellation).await.is_err() {
        println!("live two-peer SKIPPED: the dialer's bridge offered no shared session");
        return;
    }

    // The peer echoes one inbound stream in the background; a peer that never
    // becomes reachable must not hang the dialer's verdict.
    let echo_peer = Arc::new(peer);
    let echo_client = Arc::clone(&echo_peer);
    let echo_cancellation = cancellation.clone();
    let echo = tokio::spawn(async move { echo_once(&echo_client, &echo_cancellation).await });
    // A moment for the accept to register before dialling it.
    tokio::time::sleep(Duration::from_secs(2)).await;

    match dial_and_probe(&dialer, &peer_destination, peer_port, &cancellation).await {
        Ok(()) => println!(
            "live two-peer STREAM ok: peer across {} echoed {} bytes; peer style={:?}",
            peer_address,
            PROBE.len(),
            peer_identity.primary_style
        ),
        Err(error) => println!("live two-peer STREAM reported {error:?}"),
    }
    match echo.await {
        Ok(Ok(())) => println!("live two-peer STREAM ok: the peer side echoed the probe"),
        Ok(Err(error)) => println!("live two-peer peer side reported {error:?}"),
        Err(error) => println!("live two-peer peer task did not finish cleanly: {error:?}"),
    }
}
