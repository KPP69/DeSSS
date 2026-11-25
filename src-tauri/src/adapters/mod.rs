pub mod arcus;
pub mod asterdex;
pub mod book;
pub mod hyperliquid;
pub mod lighter;
pub mod market_catalog;
pub mod sync;
pub mod types;
use self::types::*;
use async_trait::async_trait;
pub type AdapterResult<T> = Result<T, AdapterError>;
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("未连接")]
    NotConnected,
    #[error("{0}")]
    Other(String),
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

/// Current time in milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// True when a WS error is the routine TLS close without close_notify
/// (`unexpected-eof`): the peer dropped the connection mid-TLS (a proxy
/// or router cutting an idle socket is the usual cause). It is a normal
/// reconnect event, not an error — log it as a warning and never let it
/// raise the console red dot.
pub(crate) fn is_tls_unexpected_eof(msg: &str) -> bool {
    msg.contains("unexpected-eof") || msg.contains("close_notify")
}

/// A JSON number-or-numeric-string as f64; anything else reads as 0.
pub(crate) fn f64_from(v: &serde_json::Value) -> f64 {
    match v {
        serde_json::Value::String(s) => s.parse().unwrap_or(0.0),
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        _ => 0.0,
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

    #[test]
    fn tls_unexpected_eof_matches_rustls_message() {
        assert!(is_tls_unexpected_eof(
            "peer closed connection without sending TLS close_notify (unexpected-eof)"
        ));
        assert!(is_tls_unexpected_eof("TLS 握手失败：unexpected-eof"));
        assert!(!is_tls_unexpected_eof("行情连接已关闭"));
        assert!(!is_tls_unexpected_eof("HTTP 429"));
    }
}
/// Unified exchange contract, implemented by the real read-only adapters
/// (Hyperliquid / Entropy / Lighter). No mock adapter remains: every
/// number the app shows comes from a real exchange read.
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
