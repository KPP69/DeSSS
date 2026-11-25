//! Unified network transport: proxy resolution, the shared HTTP client,
//! and the one WebSocket dial (direct + HTTP CONNECT proxy).
//!
//! Everything that touches the network goes through here, so a proxy or
//! TLS fix lands once instead of once per adapter.

pub mod proxy;
pub mod ws;

/// The one shared HTTP client for the whole process: a single connection
/// pool for every adapter and the market catalog. Creating a client per
/// request — or one per adapter — spawns a pool per client and exhausts
/// connections under concurrency, which surfaces as spurious `TimedOut`
/// errors. `reqwest::Client` is cheap to clone: clones share the pool.
/// 20s timeout covers the slowest endpoint (Lighter can be slow); the
/// timeout only bounds the worst case, it does not slow fast requests.
static SHARED_CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();

pub fn shared_http_client() -> reqwest::Client {
    SHARED_CLIENT.get_or_init(build_shared_client).clone()
}

fn build_shared_client() -> reqwest::Client {
    let mut b = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
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
