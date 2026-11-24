//! Local proxy resolution + proxied WebSocket dialing.
//!
//! Resolution order (first hit wins):
//! 1. custom proxy file `./data/proxy.json` (`{"url": "http://127.0.0.1:7897"}`),
//!    set from the System page;
//! 2. `HTTP_PROXY` / `HTTPS_PROXY` environment variables;
//! 3. the Windows system proxy registry
//!    (`HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings`).
//!
//! reqwest's own default only reads env vars, so the file/registry sources
//! are applied explicitly (REST in `new_http_client_with_timeout`, WS in
//! `connect_ws`). Only plain `http://` proxies are supported — that is
//! what Clash Verge exposes locally.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::handshake::client::Response;
use tokio_tungstenite::{client_async, MaybeTlsStream, WebSocketStream};

/// The WS stream type, identical in the direct and proxied paths.
pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Effective proxy URL, if any.
pub fn resolve_proxy() -> Option<String> {
    read_proxy_file().or_else(env_proxy).or_else(registry_proxy)
}

/// Env vars, either case (reqwest checks both too; this VM's egress
/// proxy, for example, is set in lowercase).
fn env_proxy() -> Option<String> {
    for k in ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"] {
        if let Ok(v) = std::env::var(k) {
            let v = v.trim().to_string();
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

fn read_proxy_file() -> Option<String> {
    let text = std::fs::read_to_string(crate::portable::data_dir().join("proxy.json")).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    v.get("url")
        .and_then(|u| u.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// The user-configured proxy from `./data/proxy.json`, if set.
pub fn read_proxy_file_pub() -> Option<String> {
    read_proxy_file()
}

/// Persist (or clear with `None`) the user-configured proxy.
pub fn write_proxy_file(url: Option<&str>) -> Result<(), String> {
    let path = crate::portable::data_dir().join("proxy.json");
    match url.map(|s| s.trim()).filter(|s| !s.is_empty()) {
        Some(u) => {
            let _ = parse_proxy_url(u)?;
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let body = serde_json::json!({ "url": u });
            std::fs::write(
                &path,
                serde_json::to_string_pretty(&body).unwrap_or_default(),
            )
            .map_err(|e| format!("failed to write proxy.json: {e}"))?;
        }
        None => {
            let _ = std::fs::remove_file(&path);
        }
    }
    Ok(())
}

/// Parse an `http://host:port` proxy URL. `https://` proxies are rejected:
/// Clash-style local proxies are plain HTTP.
fn parse_proxy_url(raw: &str) -> Result<(String, u16), String> {
    let rest = raw
        .strip_prefix("http://")
        .ok_or_else(|| "proxy url must start with http://".to_string())?;
    let hostport = rest.split(['/', '?']).next().unwrap_or("");
    let (host, port) = match hostport.split_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>()
                .map_err(|_| "proxy url needs a numeric port".to_string())?,
        ),
        None => (hostport.to_string(), 8080),
    };
    if host.is_empty() {
        return Err("proxy url needs a host".to_string());
    }
    Ok((host, port))
}

/// Parse a Windows `ProxyServer` registry value: `127.0.0.1:7897`, or a
/// per-protocol list like `http=127.0.0.1:7897;https=127.0.0.1:7897`.
fn parse_proxy_server(server: &str) -> Option<String> {
    let first = server.split(';').next()?.trim();
    if first.is_empty() {
        return None;
    }
    let addr = first.split('=').last()?.trim();
    if addr.is_empty() {
        return None;
    }
    Some(if addr.contains("://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    })
}

#[cfg(windows)]
fn registry_proxy() -> Option<String> {
    use winreg::enums::HKEY_CURRENT_USER;
    use winreg::RegKey;
    let key = RegKey::predef(HKEY_CURRENT_USER)
        .open_subkey("Software\\Microsoft\\Windows\\CurrentVersion\\Internet Settings")
        .ok()?;
    let enabled: u32 = key.get_value("ProxyEnable").ok()?;
    if enabled == 0 {
        return None;
    }
    let server: String = key.get_value("ProxyServer").ok()?;
    parse_proxy_server(&server)
}

#[cfg(not(windows))]
fn registry_proxy() -> Option<String> {
    None
}

/// Dial a WebSocket URL, through the configured proxy when one exists.
/// The direct path is exactly `connect_async`; the proxied path does a
/// manual HTTP CONNECT (then TLS for `wss://`) and the same tungstenite
/// client handshake over the resulting stream.
pub async fn connect_ws(url: &str, timeout: Duration) -> Result<(WsStream, Response), String> {
    match resolve_proxy() {
        None => tokio::time::timeout(timeout, tokio_tungstenite::connect_async(url))
            .await
            .map_err(|_| format!("ws connect to {url} timed out"))?
            .map_err(|e| format!("ws connect failed: {e}")),
        Some(proxy_url) => {
            let (host, port, tls) = parse_ws_url(url)?;
            let tcp = tokio::time::timeout(timeout, connect_via_proxy(&proxy_url, &host, port))
                .await
                .map_err(|_| format!("proxy connect to {host}:{port} timed out"))??;
            let stream = if tls {
                MaybeTlsStream::Rustls(tls_wrap(tcp, &host).await?)
            } else {
                MaybeTlsStream::Plain(tcp)
            };
            tokio::time::timeout(timeout, client_async(url, stream))
                .await
                .map_err(|_| "ws handshake timed out".to_string())?
                .map_err(|e| format!("ws handshake failed: {e}"))
        }
    }
}

fn parse_ws_url(url: &str) -> Result<(String, u16, bool), String> {
    let (tls, rest) = if let Some(r) = url.strip_prefix("wss://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("ws://") {
        (false, r)
    } else {
        return Err(format!("unsupported ws url: {url}"));
    };
    let hostport = rest.split(['/', '?']).next().unwrap_or("");
    let (host, port) = match hostport.split_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>()
                .map_err(|_| format!("bad ws port in {url}"))?,
        ),
        None => (hostport.to_string(), if tls { 443 } else { 80 }),
    };
    if host.is_empty() {
        return Err(format!("bad ws url: {url}"));
    }
    Ok((host, port, tls))
}

/// Open a TCP connection to `host:port` through an HTTP proxy's CONNECT.
async fn connect_via_proxy(proxy_url: &str, host: &str, port: u16) -> Result<TcpStream, String> {
    let (phost, pport) = parse_proxy_url(proxy_url)?;
    let mut stream = TcpStream::connect((phost.as_str(), pport))
        .await
        .map_err(|e| format!("proxy tcp connect failed ({proxy_url}): {e:?}"))?;
    let req = format!(
        "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Connection: keep-alive\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("proxy CONNECT write failed: {e}"))?;
    // Read response headers until the blank line (cap 8 KiB).
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while buf.len() < 8192 {
        let n = stream
            .read(&mut byte)
            .await
            .map_err(|e| format!("proxy CONNECT read failed: {e}"))?;
        if n == 0 {
            return Err("proxy closed the CONNECT".to_string());
        }
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let status = head.lines().next().unwrap_or("");
    if !status.contains(" 200 ") {
        return Err(format!("proxy CONNECT rejected: {}", status.trim()));
    }
    Ok(stream)
}

async fn tls_wrap(
    stream: TcpStream,
    host: &str,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, String> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
    let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| format!("bad ws host {host}: {e}"))?;
    connector
        .connect(server_name, stream)
        .await
        .map_err(|e| format!("tls handshake failed: {e:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_server_parses_plain_and_per_protocol_forms() {
        assert_eq!(
            parse_proxy_server("127.0.0.1:7897"),
            Some("http://127.0.0.1:7897".to_string())
        );
        assert_eq!(
            parse_proxy_server("http=127.0.0.1:7897;https=127.0.0.1:7897"),
            Some("http://127.0.0.1:7897".to_string())
        );
        assert_eq!(parse_proxy_server(""), None);
        assert_eq!(
            parse_proxy_server("http://127.0.0.1:7897"),
            Some("http://127.0.0.1:7897".to_string())
        );
    }

    #[test]
    fn proxy_url_requires_http_scheme_and_port() {
        assert_eq!(
            parse_proxy_url("http://127.0.0.1:7897").unwrap(),
            ("127.0.0.1".to_string(), 7897)
        );
        assert!(parse_proxy_url("https://127.0.0.1:7897").is_err());
        assert!(parse_proxy_url("127.0.0.1:7897").is_err());
        assert!(parse_proxy_url("http://127.0.0.1:abc").is_err());
    }

    #[test]
    fn proxy_file_roundtrips_and_clears() {
        // Env vars are process-global: save, clear, restore.
        let saved: Vec<(String, Option<String>)> =
            ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"]
                .iter()
                .map(|k| (k.to_string(), std::env::var(k).ok()))
                .collect();
        for k in ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"] {
            std::env::remove_var(k);
        }
        write_proxy_file(Some("http://127.0.0.1:7897")).expect("write");
        assert_eq!(resolve_proxy(), Some("http://127.0.0.1:7897".to_string()));
        write_proxy_file(None).expect("clear");
        assert_eq!(resolve_proxy(), None);
        for (k, v) in saved {
            match v {
                Some(v) => std::env::set_var(&k, v),
                None => std::env::remove_var(&k),
            }
        }
    }

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
        let stream = connect_via_proxy(&proxy_url, "api.rh.lighter.xyz", 443)
            .await
            .expect("connect");
        drop(stream);
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
}
