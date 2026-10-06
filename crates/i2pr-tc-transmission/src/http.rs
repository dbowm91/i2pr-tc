//! One-request HTTP adapter over a caller-owned bidirectional stream.
use crate::{
    adapter::{TransmissionAdapter, encode_response},
    wire::{self, WireMode},
};
use serde_json::json;
use std::{io, sync::Arc};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_HTTP_HEADER_BYTES: usize = 32 * 1024;
const MAX_HTTP_HEADERS: usize = 64;
const SESSION_HEADER: &str = "x-transmission-session-id";

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("HTTP stream I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("HTTP request is malformed or outside configured bounds")]
    Protocol,
}

#[derive(Clone)]
pub struct RpcEndpoint {
    adapter: Arc<TransmissionAdapter>,
    session_id: String,
}

impl RpcEndpoint {
    /// `session_id` is supplied by the runtime owner and should be fresh and
    /// unpredictable for each application process.
    pub fn new(adapter: Arc<TransmissionAdapter>, session_id: String) -> Result<Self, HttpError> {
        if !(16..=256).contains(&session_id.len())
            || !session_id.bytes().all(|byte| byte.is_ascii_graphic())
            || session_id.contains(['\r', '\n', '"'])
        {
            return Err(HttpError::Protocol);
        }
        Ok(Self {
            adapter,
            session_id,
        })
    }

    /// Handles exactly one request and leaves connection ownership to the host.
    /// The stream can come from a private app-local ingress; this crate never
    /// binds a listener or opens a host-network socket.
    pub async fn handle_connection<S>(&self, mut stream: S) -> Result<(), HttpError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let request = read_request(&mut stream).await?;
        if request.session_id.as_deref() != Some(&self.session_id) {
            return write_response(
                &mut stream,
                409,
                "Conflict",
                b"",
                &[
                    ("X-Transmission-Session-Id", self.session_id.as_str()),
                    (
                        "X-Transmission-Rpc-Version",
                        crate::adapter::RPC_VERSION_SEMVER,
                    ),
                ],
            )
            .await;
        }
        if request.method != "POST" {
            return write_response(&mut stream, 405, "Method Not Allowed", b"", &[]).await;
        }
        if !matches!(
            request.path.as_str(),
            "/transmission/rpc" | "/transmission/rpc/"
        ) {
            return write_response(&mut stream, 404, "Not Found", b"", &[]).await;
        }
        let response = match self.adapter.handle(&request.body) {
            Ok(response) => response,
            Err(_) => return write_response(&mut stream, 400, "Bad Request", b"", &[]).await,
        };
        if response.mode == WireMode::JsonRpc && response.id.is_none() {
            return write_response(&mut stream, 204, "No Content", b"", &[]).await;
        }
        let body = encode_response(&response).map_err(|_| HttpError::Protocol)?;
        write_response(
            &mut stream,
            200,
            "OK",
            &body,
            &[(
                "X-Transmission-Rpc-Version",
                crate::adapter::RPC_VERSION_SEMVER,
            )],
        )
        .await
    }
}

struct HttpRequest {
    method: String,
    path: String,
    session_id: Option<String>,
    body: Vec<u8>,
}

async fn read_request<R: AsyncRead + Unpin>(reader: &mut R) -> Result<HttpRequest, HttpError> {
    let mut bytes = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            return Err(HttpError::Protocol);
        }
        if bytes.len().saturating_add(read) > MAX_HTTP_HEADER_BYTES + wire::MAX_REQUEST_BYTES {
            return Err(HttpError::Protocol);
        }
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            if position + 4 > MAX_HTTP_HEADER_BYTES {
                return Err(HttpError::Protocol);
            }
            break position;
        }
        if bytes.len() > MAX_HTTP_HEADER_BYTES {
            return Err(HttpError::Protocol);
        }
    };
    let headers = std::str::from_utf8(&bytes[..header_end]).map_err(|_| HttpError::Protocol)?;
    let mut lines = headers.split("\r\n");
    let mut request_line = lines
        .next()
        .ok_or(HttpError::Protocol)?
        .split_ascii_whitespace();
    let method = request_line.next().ok_or(HttpError::Protocol)?.to_owned();
    let path = request_line.next().ok_or(HttpError::Protocol)?.to_owned();
    let version = request_line.next().ok_or(HttpError::Protocol)?;
    if request_line.next().is_some() || !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        return Err(HttpError::Protocol);
    }
    let mut content_length = None;
    let mut session_id = None;
    let mut count = 0;
    for line in lines {
        count += 1;
        if count > MAX_HTTP_HEADERS {
            return Err(HttpError::Protocol);
        }
        let (name, value) = line.split_once(':').ok_or(HttpError::Protocol)?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            let length = value.parse::<usize>().map_err(|_| HttpError::Protocol)?;
            if content_length.replace(length).is_some() || length > wire::MAX_REQUEST_BYTES {
                return Err(HttpError::Protocol);
            }
        } else if name.eq_ignore_ascii_case(SESSION_HEADER) {
            if session_id.replace(value.to_owned()).is_some() {
                return Err(HttpError::Protocol);
            }
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(HttpError::Protocol);
        }
    }
    let length = content_length.ok_or(HttpError::Protocol)?;
    let body_start = header_end + 4;
    let available = bytes.len().saturating_sub(body_start);
    if available > length {
        return Err(HttpError::Protocol);
    }
    let mut body = bytes[body_start..].to_vec();
    body.resize(length, 0);
    if available < length {
        reader.read_exact(&mut body[available..]).await?;
    }
    Ok(HttpRequest {
        method,
        path,
        session_id,
        body,
    })
}

async fn write_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    status: u16,
    phrase: &str,
    body: &[u8],
    headers: &[(&str, &str)],
) -> Result<(), HttpError> {
    let mut response = format!(
        "HTTP/1.1 {status} {phrase}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if !body.is_empty() {
        response.push_str("Content-Type: application/json\r\n");
    }
    for (name, value) in headers {
        if value.contains(['\r', '\n']) {
            return Err(HttpError::Protocol);
        }
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    writer.write_all(response.as_bytes()).await?;
    writer.write_all(body).await?;
    writer.flush().await?;
    Ok(())
}

pub fn invalid_request_response(mode: WireMode) -> Result<Vec<u8>, wire::WireError> {
    let body = match mode {
        WireMode::JsonRpc => {
            json!({"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"invalid request"}})
        }
        WireMode::Legacy => {
            json!({"result":"failure","arguments":{"result":"invalid request"},"tag":null})
        }
    };
    serde_json::to_vec(&body).map_err(|_| wire::WireError::Invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{adapter::TransmissionAdapter, ids::RpcIdStore};
    use i2pr_tc_storage::{Cancellation, TorrentRuntime};
    use std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    fn root() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "i2pr-tc-rpc-http-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn endpoint(root: &std::path::Path) -> RpcEndpoint {
        let data = root.join("payload");
        std::fs::create_dir_all(&data).unwrap();
        let runtime =
            TorrentRuntime::open(root.join("catalog"), &Cancellation::default(), 4, 8, 4).unwrap();
        let adapter = TransmissionAdapter::new(
            Arc::new(runtime),
            Arc::new(RpcIdStore::open(root.join("ids.json")).unwrap()),
            data,
        )
        .unwrap();
        RpcEndpoint::new(Arc::new(adapter), "session-token-1234567890".into()).unwrap()
    }

    async fn request(endpoint: &RpcEndpoint, request: Vec<u8>) -> Vec<u8> {
        let (client, server) = duplex(64 * 1024);
        let endpoint = endpoint.clone();
        let server = tokio::spawn(async move { endpoint.handle_connection(server).await });
        let mut client = client;
        client.write_all(&request).await.unwrap();
        client.shutdown().await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        server.await.unwrap().unwrap();
        response
    }

    #[tokio::test]
    async fn csrf_token_negotiation_returns_409_then_json_rpc_200() {
        let root = root();
        let endpoint = endpoint(&root);
        let body =
            br#"{"jsonrpc":"2.0","method":"session_get","params":{"fields":["version"]},"id":1}"#;
        let first = format!(
            "POST /transmission/rpc HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        let mut request_bytes = first.into_bytes();
        request_bytes.extend_from_slice(body);
        let response = request(&endpoint, request_bytes).await;
        assert!(response.starts_with(b"HTTP/1.1 409 Conflict\r\n"));
        let session_header = b"X-Transmission-Session-Id: session-token-1234567890";
        assert!(
            response
                .windows(session_header.len())
                .any(|window| window == session_header)
        );

        let header = format!(
            "POST /transmission/rpc HTTP/1.1\r\nX-Transmission-Session-Id: session-token-1234567890\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        let mut request_bytes = header.into_bytes();
        request_bytes.extend_from_slice(body);
        let response = request(&endpoint, request_bytes).await;
        assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
        assert!(response.windows(7).any(|window| window == b"i2pr-tc"));
        drop(endpoint);
        let _ = std::fs::remove_dir_all(root);
    }
}
