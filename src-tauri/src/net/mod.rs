//! Unified network transport: proxy resolution, the shared HTTP client,
//! and the one WebSocket dial (direct + HTTP CONNECT proxy).
//!
//! Everything that touches the network goes through here, so a proxy or
//! TLS fix lands once instead of once per adapter.

pub mod proxy;
pub mod ws;

/// Shared HTTP client for the read-only adapters: system-native root
/// certificates (so corporate proxies / system TLS works), a standard
/// User-Agent, and a 10s timeout. reqwest reads HTTP(S)_PROXY from the
/// environment by default, so the system proxy is honoured.
pub fn new_http_client() -> reqwest::Client {
    new_http_client_with_timeout(10)
}

/// Same client with a custom REST timeout (Lighter / RH use 20s — their
/// endpoints can be slow). Connection pooling stays at the reqwest default
/// (unlimited idle per host); nothing here disables it. When a proxy is
/// configured (custom file, env vars, or the Windows system proxy), it is
/// applied explicitly — reqwest's own default only reads env vars.
pub fn new_http_client_with_timeout(secs: u64) -> reqwest::Client {
    let mut b = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(secs))
        .user_agent("DeSSS/0.1.0");
    if let Some(url) = proxy::resolve_proxy() {
        match reqwest::Proxy::all(&url) {
            Ok(p) => {
                b = b.proxy(p);
            }
            Err(e) => {
                tracing::warn!("[proxy] ignoring invalid proxy url {url}: {e}");
            }
        }
    }
    b.build().unwrap_or_default()
}
