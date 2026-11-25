pub mod arcus;
pub mod asterdex;
pub mod book;
pub mod hl_signing;
pub mod hyperliquid;
pub mod lighter;
pub mod market_catalog;
pub mod reconcile;
pub mod sync;
pub mod types;
use self::types::*;
use async_trait::async_trait;
pub type AdapterResult<T> = Result<T, AdapterError>;
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("未连接")]
    NotConnected,
    /// The request was sent (or may have been sent) but no usable
    /// answer came back — timeout, transport failure, unreadable
    /// response. The outcome on the exchange is UNKNOWN: the caller
    /// must reconcile by querying the order, never guess filled or
    /// failed. Only write paths produce this; reads use `Other`.
    #[error("结果未知，需对账：{0}")]
    UnknownOutcome(String),
    #[error("{0}")]
    Other(String),
}

/// Account live status from REST health only (there is no market WS on
/// an account anymore — account sync is REST polling, and public market
/// data comes from the on-demand catalog):
/// - REST ok → "connected" (latency from the last REST round-trip)
/// - REST down → "error" when the last failure is known, else "connecting"
pub fn live_status(rest_ok: bool, has_error: bool) -> &'static str {
    if rest_ok {
        "connected"
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
    fn live_status_from_rest_only() {
        assert_eq!(live_status(true, false), "connected");
        assert_eq!(live_status(true, true), "connected");
        assert_eq!(live_status(false, true), "error");
        assert_eq!(live_status(false, false), "connecting");
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
    /// Cancel one order. `order_id` is the exchange oid, or a cloid
    /// (`0x…`); `symbol` is needed to resolve the asset index.
    /// `Ok(())` means the exchange confirmed the cancel. A definite
    /// rejection (already filled / already canceled / never placed)
    /// is `Err(Other(..))` — the caller queries the order to find
    /// out which. A send whose outcome is unknown is
    /// `Err(UnknownOutcome(..))` — the caller must query as well.
    async fn cancel_order(
        &self,
        account_id: &str,
        order_id: &str,
        symbol: &str,
    ) -> AdapterResult<()>;
    /// Authoritative order state by cloid (orderStatus + fills).
    /// `Err(Other("unknownOid"))` means the exchange has no such
    /// order — the caller keeps the order Unresolved, it does NOT
    /// mean the order was never placed. Default: the exchange does
    /// not support ordering yet.
    async fn query_order_by_cloid(
        &self,
        _account_id: &str,
        _cloid: &str,
    ) -> AdapterResult<OrderInfo> {
        Err(AdapterError::Other(
            "该交易所下单将在下一阶段接入".to_string(),
        ))
    }
    /// Fills of one order, by cloid (userFillsByTime matched by oid).
    /// Default: the exchange does not support ordering yet.
    async fn query_fills(&self, _account_id: &str, _cloid: &str) -> AdapterResult<Vec<TradeInfo>> {
        Err(AdapterError::Other(
            "该交易所下单将在下一阶段接入".to_string(),
        ))
    }
    /// Long-running read loop (WS + REST polling). Default is a no-op; real
    /// adapters override it. The caller spawns it once after connect().
    async fn run(&self) {}

    /// Wake the poll loop for an immediate account sync (position
    /// reconciliation). Default is a no-op; adapters with a REST loop
    /// override it to notify their loop.
    fn wake_sync(&self) {}
}
