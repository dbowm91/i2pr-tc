//! Test-only loopback qualification against the installed Transmission client.
//!
//! boundary-guard:test-only
use i2pr_tc_storage::{Cancellation, TorrentRuntime};
use i2pr_tc_transmission::{adapter::TransmissionAdapter, http::RpcEndpoint, ids::RpcIdStore};
use std::{
    path::PathBuf,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{net::TcpListener, sync::oneshot};

fn root() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "i2pr-tc-transmission-remote-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

#[tokio::test(flavor = "multi_thread")]
async fn transmission_remote_lists_native_torrent_through_test_only_loopback() {
    let Ok(version) = Command::new("transmission-remote")
        .arg("--version")
        .output()
    else {
        return;
    };
    if !version.status.success() {
        return;
    }
    let root = root();
    let payload = root.join("payload");
    std::fs::create_dir_all(&payload).unwrap();
    let runtime =
        TorrentRuntime::open(root.join("catalog"), &Cancellation::default(), 8, 16, 4).unwrap();
    let mut metainfo = b"d4:infod6:lengthi4e4:name1:x12:piece lengthi4e6:pieces20:".to_vec();
    metainfo.extend([0u8; 20]);
    metainfo.extend_from_slice(b"ee");
    runtime.add_metainfo(&metainfo).unwrap();
    let adapter = TransmissionAdapter::new(
        Arc::new(runtime),
        Arc::new(RpcIdStore::open(root.join("rpc-ids.json")).unwrap()),
        payload,
    )
    .unwrap();
    let endpoint =
        RpcEndpoint::new(Arc::new(adapter), "transmission-remote-test-token".into()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else { break; };
                    let endpoint = endpoint.clone();
                    tokio::spawn(async move { let _ = endpoint.handle_connection(stream).await; });
                }
            }
        }
    });
    let url = format!("http://{address}/transmission");
    let output = tokio::task::spawn_blocking(move || {
        Command::new("transmission-remote")
            .arg(&url)
            .arg("--list")
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let _ = stop_tx.send(());
    server.await.unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let _ = std::fs::remove_dir_all(root);
}
