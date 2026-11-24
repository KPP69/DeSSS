pub mod book;
pub mod hyperliquid;
pub mod lighter;
pub mod mock;
pub mod sync;
pub mod types;
use self::types::*;
use async_trait::async_trait;
pub type AdapterResult<T> = Result<T, AdapterError>;
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("not connected")]
    NotConnected,
    #[error("{0}")]
    Other(String),
}

/// Shared HTTP client for the read-only adapters: system-native root
/// certificates (so corporate proxies / system TLS works), a standard
/// User-Agent, and a 10s timeout. reqwest reads HTTP(S)_PROXY from the
/// environment by default, so the system proxy is honoured.
pub fn new_http_client() -> reqwest::Client {
    new_http_client_with_timeout(10)
}

/// Same client with a custom REST timeout (Lighter / RH use 20s — their
/// endpoints can be slow). Connection pooling stays at the reqwest default
/// (unlimited idle per host); nothing here disables it.
pub fn new_http_client_with_timeout(secs: u64) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(secs))
        .user_agent("DeSSS/0.1.0")
        .build()
        .unwrap_or_default()
}

/// Account live status from the two independent health signals, so a dead
/// market WS never hides a healthy REST account read:
/// - WS up → "connected" (latency from the WS ping/pong RTT)
/// - WS down but REST ok → "rest-only" (equity/positions still real)
/// - both down → "error" when the last failure is known, else "connecting"
pub fn live_status(ws_up: bool, rest_ok: bool, has_error: bool) -> &'static str {
    if ws_up {
        "connected"
    } else if rest_ok {
        "rest-only"
    } else if has_error {
        "error"
    } else {
        "connecting"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_status_prefers_ws_then_rest() {
        assert_eq!(live_status(true, true, false), "connected");
        assert_eq!(live_status(true, false, true), "connected");
        assert_eq!(live_status(false, true, true), "rest-only");
        assert_eq!(live_status(false, false, true), "error");
        assert_eq!(live_status(false, false, false), "connecting");
    }
}
/// Unified exchange contract. No real exchange is implemented in scaffold phase; only MockAdapter.
#[async_trait]
pub trait ExchangeAdapter: Send + Sync {
    fn id(&self) -> &str;
    fn name(&self) -> &str;
    async fn connect(&self) -> AdapterResult<()>;
    async fn disconnect(&self) -> AdapterResult<()>;
    async fn subscribe_ticks(&self, symbols: Vec<String>) -> AdapterResult<()>;
    async fn unsubscribe_ticks(&self, symbols: Vec<String>) -> AdapterResult<()>;
    async fn get_balances(&self, account_id: &str) -> AdapterResult<Vec<Balance>>;
    async fn get_positions(&self, account_id: &str) -> AdapterResult<Vec<Position>>;
    async fn get_open_orders(&self, account_id: &str) -> AdapterResult<Vec<Order>>;
    async fn place_order(&self, req: OrderRequest) -> AdapterResult<Order>;
    async fn cancel_order(&self, account_id: &str, order_id: &str) -> AdapterResult<()>;
    /// Long-running read loop (WS + REST polling). Default is a no-op; real
    /// adapters override it. The caller spawns it once after connect().
    async fn run(&self) {}
}
