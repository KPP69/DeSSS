//! One WebSocket dial for direct and HTTP CONNECT proxy paths.
//!
//! The proxy handshake reads the CONNECT response with a `BufReader` and
//! the SAME buffered reader is handed to the TLS layer and then to
//! tungstenite — a `BufReader` may already hold bytes past the `200`
//! headers (the TLS ServerHello); dropping it and reusing the raw socket
//! would swallow those bytes and the TLS handshake would hang forever
//! with no error. Every stage (CONNECT, TLS, WS handshake) has its own
//! timeout, so a stuck stage can never hang the adapter silently.
//!
//! TLS: webpki roots, SNI from the WS host, ALPN forced to `http/1.1`
//! (a WebSocket Upgrade is HTTP/1.1 — negotiating `h2` breaks the
//! tungstenite handshake).

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::handshake::client::Response;
use tokio_tungstenite::{client_async, MaybeTlsStream, WebSocketStream};

use crate::bus::EventBus;

/// Boxed WS stream: the direct and proxied paths build different concrete
/// stream types (`TcpStream` vs a `BufReader` kept from the CONNECT
/// handshake), so both are erased to one type here. (`Unpin` is not an
/// auto trait, so it cannot sit in a `dyn` bound directly — the helper
/// trait carries it as a supertrait.)
pub trait WsIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> WsIo for T {}

pub type WsStream = WebSocketStream<Box<dyn WsIo>>;

/// Dial a WebSocket URL through the configured proxy when one exists.
/// If the proxied attempt fails, try direct once as a fallback and log
/// both outcomes — a proxy outage must never silently kill every WS.
/// Every failure is logged to the EventBus as an error at the point it
/// happens, so the in-app console always shows the cause.
pub async fn connect_ws(
    url: &str,
    timeout: Duration,
    bus: &EventBus,
) -> Result<(WsStream, Response), String> {
    let proxy = super::proxy::resolve_proxy();
    match connect_ws_with_proxy(url, proxy.as_deref(), timeout).await {
        Ok(v) => Ok(v),
        Err(e) => {
            if let Some(p) = &proxy {
                bus.log(
                    "error",
                    "net",
                    format!("代理 {p} 连接行情失败：{e}，改用直连"),
                );
                match connect_ws_with_proxy(url, None, timeout).await {
                    Ok(v) => {
                        bus.log("warn", "net", format!("代理失败后已直连行情：{e}"));
                        Ok(v)
                    }
                    Err(e2) => Err(format!("行情连接失败：代理（{e}）与直连（{e2}）均失败")),
                }
            } else {
                bus.log("error", "net", format!("行情连接失败：{e}"));
                Err(e)
            }
        }
    }
}

/// Dial with an explicit proxy (None = direct). Testable without env vars
/// or the proxy file.
pub async fn connect_ws_with_proxy(
    url: &str,
    proxy: Option<&str>,
    timeout: Duration,
) -> Result<(WsStream, Response), String> {
    let (host, port, tls) = parse_ws_url(url)?;
    let stream: Box<dyn WsIo> = match proxy {
        Some(p) => {
            let reader = tokio::time::timeout(timeout, connect_via_proxy(p, &host, port))
                .await
                .map_err(|_| format!("代理 CONNECT 超时：{host}:{port}"))??;
            if tls {
                let tls_stream = tokio::time::timeout(timeout, tls_wrap(reader, &host))
                    .await
                    .map_err(|_| "TLS 握手超时".to_string())??;
                Box::new(MaybeTlsStream::Rustls(tls_stream))
            } else {
                Box::new(MaybeTlsStream::Plain(reader))
            }
        }
        None => {
            let tcp = tokio::time::timeout(timeout, TcpStream::connect((host.as_str(), port)))
                .await
                .map_err(|_| format!("TCP 连接超时：{host}:{port}"))?
                .map_err(|e| format!("TCP 连接失败：{e:?}"))?;
            if tls {
                let tls_stream =
                    tokio::time::timeout(timeout, tls_wrap(BufReader::new(tcp), &host))
                        .await
                        .map_err(|_| "TLS 握手超时".to_string())??;
                Box::new(MaybeTlsStream::Rustls(tls_stream))
            } else {
                Box::new(MaybeTlsStream::Plain(tcp))
            }
        }
    };
    let (ws, resp) = tokio::time::timeout(timeout, client_async(url, stream))
        .await
        .map_err(|_| "行情握手超时".to_string())?
        .map_err(|e| format!("行情握手失败：{e}"))?;
    Ok((ws, resp))
}

fn parse_ws_url(url: &str) -> Result<(String, u16, bool), String> {
    let (tls, rest) = if let Some(r) = url.strip_prefix("wss://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("ws://") {
        (false, r)
    } else {
        return Err(format!("不支持的行情地址：{url}"));
    };
    let hostport = rest.split(['/', '?']).next().unwrap_or("");
    let (host, port) = match hostport.split_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>()
                .map_err(|_| format!("行情地址端口无效：{url}"))?,
        ),
        None => (hostport.to_string(), if tls { 443 } else { 80 }),
    };
    if host.is_empty() {
        return Err(format!("行情地址无效：{url}"));
    }
    Ok((host, port, tls))
}

/// Open a TCP connection to `host:port` through an HTTP proxy's CONNECT.
/// Returns the buffered reader — it may already hold bytes past the `200`
/// headers, so the caller must keep using THIS reader for TLS, never the
/// raw socket underneath it.
async fn connect_via_proxy(
    proxy_url: &str,
    host: &str,
    port: u16,
) -> Result<BufReader<TcpStream>, String> {
    let (phost, pport) = super::proxy::parse_proxy_url(proxy_url)?;
    let stream = TcpStream::connect((phost.as_str(), pport))
        .await
        .map_err(|e| format!("代理 TCP 连接失败（{proxy_url}）：{e:?}"))?;
    let mut reader = BufReader::new(stream);
    let req = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n");
    reader
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("代理 CONNECT 写入失败：{e}"))?;
    reader
        .flush()
        .await
        .map_err(|e| format!("代理 CONNECT 发送失败：{e}"))?;
    // Status line first, then the remaining headers until the blank line.
    let mut status = String::new();
    reader
        .read_line(&mut status)
        .await
        .map_err(|e| format!("代理 CONNECT 读取失败：{e}"))?;
    if status.is_empty() {
        return Err("代理关闭了 CONNECT 连接".to_string());
    }
    if !status.contains(" 200 ") {
        return Err(format!("代理拒绝 CONNECT：{}", status.trim()));
    }
    loop {
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .map_err(|e| format!("代理 CONNECT 读取失败：{e}"))?;
        if line == "\r\n" || line == "\n" || line.is_empty() {
            break;
        }
    }
    Ok(reader)
}

/// One global ring CryptoProvider, initialized exactly once (OnceLock is
/// the std::sync::Once-based one-time init): building it per dial is
/// wasteful, and a process-default install would fail on the second call.
static CRYPTO_PROVIDER: std::sync::OnceLock<std::sync::Arc<rustls::crypto::CryptoProvider>> =
    std::sync::OnceLock::new();

fn crypto_provider() -> std::sync::Arc<rustls::crypto::CryptoProvider> {
    CRYPTO_PROVIDER
        .get_or_init(|| std::sync::Arc::new(rustls::crypto::ring::default_provider()))
        .clone()
}

fn tls_client_config() -> rustls::ClientConfig {
    // Pick the crypto provider explicitly: with both `ring` and
    // `aws-lc-rs` enabled (reqwest pulls the latter in), the plain
    // `ClientConfig::builder()` cannot auto-detect one and panics.
    let provider = crypto_provider();
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .expect("tls versions")
        .with_root_certificates(tls_roots())
        .with_no_client_auth();
    // WebSocket is an HTTP/1.1 Upgrade — never negotiate h2.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config
}

fn tls_roots() -> rustls::RootCertStore {
    let mut store = rustls::RootCertStore::empty();
    store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    #[cfg(test)]
    for r in TEST_TLS_ROOTS.lock().unwrap().iter() {
        let _ = store.add(r.clone());
    }
    store
}

#[cfg(test)]
static TEST_TLS_ROOTS: std::sync::Mutex<Vec<rustls::pki_types::CertificateDer<'static>>> =
    std::sync::Mutex::new(Vec::new());

/// Test-only: trust extra roots (e.g. a self-signed cert from rcgen) in
/// addition to the webpki roots. Roots accumulate across tests: two
/// parallel tests each spin up their own TLS server with its own cert,
/// and overwriting the list here used to un-trust the other test's cert
/// mid-handshake (the parallel `cargo test` flake).
#[cfg(test)]
pub fn set_test_tls_roots(roots: Vec<rustls::pki_types::CertificateDer<'static>>) {
    TEST_TLS_ROOTS.lock().unwrap().extend(roots);
}

async fn tls_wrap<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    host: &str,
) -> Result<tokio_rustls::client::TlsStream<S>, String> {
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(tls_client_config()));
    let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| format!("行情地址无效 {host}：{e}"))?;
    connector
        .connect(server_name, stream)
        .await
        .map_err(|e| format!("TLS 握手失败：{e:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio_tungstenite::tungstenite::Message;

    #[tokio::test]
    async fn connect_via_proxy_sends_connect_and_accepts_200() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            let n = sock.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..n]).to_string();
            sock.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .unwrap();
            req
        });
        let proxy_url = format!("http://127.0.0.1:{}", addr.port());
        let reader = connect_via_proxy(&proxy_url, "api.rh.lighter.xyz", 443)
            .await
            .expect("connect");
        drop(reader);
        let req = server.await.unwrap();
        assert!(
            req.starts_with("CONNECT api.rh.lighter.xyz:443 HTTP/1.1\r\n"),
            "unexpected CONNECT request: {req}"
        );
    }

    #[tokio::test]
    async fn connect_via_proxy_rejects_non_200() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            let _ = sock.read(&mut buf).await;
            sock.write_all(b"HTTP/1.1 403 Forbidden\r\n\r\n")
                .await
                .unwrap();
        });
        let proxy_url = format!("http://127.0.0.1:{}", addr.port());
        let err = connect_via_proxy(&proxy_url, "api.rh.lighter.xyz", 443)
            .await
            .unwrap_err();
        assert!(err.contains("403"), "unexpected error: {err}");
    }

    /// Full chain: local TLS WebSocket server behind a mock HTTP CONNECT
    /// proxy. The client dials through the proxy, the TLS handshake and
    /// the WS handshake must complete, and a text frame must arrive.
    /// The mock proxy forwards raw bytes between the client and the TLS
    /// server, exactly like Clash Verge does.
    #[tokio::test]
    async fn ws_over_proxy_full_chain_receives_text_frame() {
        let (tls_addr, proxy_addr) = start_tls_ws_server_and_proxy().await;
        let proxy_url = format!("http://127.0.0.1:{}", proxy_addr.port());
        let ws_url = format!("wss://127.0.0.1:{}/", tls_addr.port());
        let (mut ws, _) = connect_ws_with_proxy(&ws_url, Some(&proxy_url), Duration::from_secs(10))
            .await
            .expect("ws over proxy must connect");
        let msg = tokio::time::timeout(
            Duration::from_secs(5),
            futures_util::StreamExt::next(&mut ws),
        )
        .await
        .expect("read timed out")
        .expect("stream ended")
        .expect("ws frame");
        assert_eq!(msg, Message::Text("hello-proxy".into()));
    }

    /// Same TLS server, direct dial (no proxy) — the other half of the
    /// unified dial function.
    #[tokio::test]
    async fn ws_direct_to_tls_server_receives_text_frame() {
        let (tls_addr, _proxy_addr) = start_tls_ws_server_and_proxy().await;
        let ws_url = format!("wss://127.0.0.1:{}/", tls_addr.port());
        let (mut ws, _) = connect_ws_with_proxy(&ws_url, None, Duration::from_secs(10))
            .await
            .expect("direct ws must connect");
        let msg = tokio::time::timeout(
            Duration::from_secs(5),
            futures_util::StreamExt::next(&mut ws),
        )
        .await
        .expect("read timed out")
        .expect("stream ended")
        .expect("ws frame");
        assert_eq!(msg, Message::Text("hello-proxy".into()));
    }

    /// Spins up (1) a TLS WebSocket server on 127.0.0.1 that sends one
    /// text frame, and (2) a mock HTTP CONNECT proxy that forwards bytes
    /// to it. Returns (tls server addr, proxy addr). The server uses a
    /// fresh rcgen self-signed cert for 127.0.0.1, trusted via
    /// set_test_tls_roots.
    async fn start_tls_ws_server_and_proxy() -> (std::net::SocketAddr, std::net::SocketAddr) {
        let certified =
            rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).expect("rcgen cert");
        let cert_der = certified.cert.der().clone();
        let key_der = certified.key_pair.serialize_der();
        set_test_tls_roots(vec![cert_der.clone()]);

        let server_config = rustls::ServerConfig::builder_with_provider(crypto_provider())
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .expect("tls versions")
            .with_no_client_auth()
            .with_single_cert(
                vec![cert_der],
                rustls::pki_types::PrivateKeyDer::Pkcs8(key_der.into()),
            )
            .expect("server cert");
        let tls_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tls_addr = tls_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = tls_listener.accept().await.unwrap();
            let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server_config));
            let tls = acceptor.accept(tcp).await.expect("server tls");
            let mut ws = tokio_tungstenite::accept_async(tls)
                .await
                .expect("server ws");
            use futures_util::SinkExt;
            ws.send(Message::Text("hello-proxy".into()))
                .await
                .expect("server send");
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (client_sock, _) = proxy_listener.accept().await.unwrap();
            let mut reader = BufReader::new(client_sock);
            // CONNECT request line + headers, then 200.
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("proxy read");
            assert!(
                line.starts_with("CONNECT 127.0.0.1:"),
                "bad CONNECT line: {line}"
            );
            loop {
                let mut l = String::new();
                reader.read_line(&mut l).await.expect("proxy read");
                if l == "\r\n" || l == "\n" {
                    break;
                }
            }
            reader
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .expect("proxy write");
            let mut client_sock = reader.into_inner();
            let mut server_sock = TcpStream::connect(tls_addr).await.expect("proxy dial");
            let _ = tokio::io::copy_bidirectional(&mut client_sock, &mut server_sock).await;
        });

        (tls_addr, proxy_addr)
    }
}
