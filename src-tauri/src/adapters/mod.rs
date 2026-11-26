pub mod hl_signing;
pub mod market_catalog;
pub mod reconcile;
pub mod sync;
use crate::core::types::*;
use async_trait::async_trait;
pub type AdapterResult<T> = Result<T, AdapterError>;

/// One error type for every venue, with a kind (v3 §2.1). Callers
/// decide "was it sent?" with [`VenueError::pre_send`], never by
/// matching error text.
#[derive(Debug, thiserror::Error)]
pub enum VenueError {
    /// No live connection to the exchange.
    #[error("未连接")]
    NotConnected,
    /// The exchange explicitly refused the request before acting on
    /// it (bad params, insufficient margin, unknown order for a
    /// cancel): definitely NOT sent.
    #[error("交易所拒绝：{msg}")]
    Rejected {
        /// Exchange error code, when the exchange gave one.
        code: Option<i64>,
        msg: String,
    },
    /// The request was sent (or may have been sent) but no usable
    /// answer came back — timeout, transport failure, unreadable
    /// response. The outcome on the exchange is UNKNOWN: the caller
    /// must reconcile by querying the order, never guess filled or
    /// failed. Only write paths produce this; reads use `Other`
    /// until they migrate (PR-1.4).
    #[error("结果未知，需对账：{0}")]
    UnknownOutcome(String),
    /// The venue does not implement this capability yet.
    #[error("该交易所暂不支持：{0}")]
    Unsupported(&'static str),
    /// Transport failure talking to the exchange (connect, DNS, TLS,
    /// non-2xx, unreadable body). The outcome is unknown.
    #[error("传输失败：{0}")]
    Transport(String),
    /// Transitional: the old catch-all. Kept ONLY so the existing
    /// `AdapterError::Other(..)` call sites keep compiling in this
    /// move-only round — no new code may use it, and it is deleted
    /// in PR-1.4 when those sites migrate to `Rejected` /
    /// `Unsupported` / `Transport` (v3 §2.1).
    #[error("{0}")]
    Other(String),
}

/// `AdapterError` is the same type as `VenueError` (v3 §2.1); the old
/// name stays until the call sites migrate in PR-1.4.
pub type AdapterError = VenueError;

impl VenueError {
    /// True only when the error proves the request was NOT sent (or
    /// was rejected before acting): the caller must NOT reconcile.
    /// Every other variant means the outcome is unknown — the caller
    /// must query the exchange.
    pub fn pre_send(&self) -> bool {
        matches!(
            self,
            VenueError::NotConnected | VenueError::Rejected { .. } | VenueError::Unsupported(_)
        )
    }
}

/// The exchange's exact "no such order" signal (orderStatus
/// `"status": "unknownOid"`). The single source of truth for the
/// "this order does not exist" decision: producers return it,
/// consumers match on it, and nothing else may invent the string.
pub const UNKNOWN_OID: &str = "unknownOid";

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

    #[test]
    fn pre_send_only_for_definite_not_sent() {
        assert!(VenueError::NotConnected.pre_send());
        assert!(VenueError::Rejected {
            code: Some(1001),
            msg: "bad params".to_string()
        }
        .pre_send());
        assert!(VenueError::Unsupported("order").pre_send());
        assert!(!VenueError::UnknownOutcome("timeout".to_string()).pre_send());
        assert!(!VenueError::Transport("reset".to_string()).pre_send());
        // The transitional catch-all is ambiguous by construction:
        // treat it as unknown, never as proof of not-sent.
        assert!(!VenueError::Other("?".to_string()).pre_send());
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
    /// Authoritative order state by numeric exchange oid
    /// (orderStatus). Same `unknownOid` contract as
    /// `query_order_by_cloid`. This is a separate method on purpose:
    /// the `oid` field must be posted as a JSON **number** — a
    /// decimal oid sent as a string is rejected by the exchange with
    /// "Failed to deserialize the JSON body into the target type"
    /// (verified live 2026-11-26), so the two id forms must never
    /// share one code path. Default: the exchange does not support
    /// ordering yet.
    async fn query_order_by_oid(&self, _account_id: &str, _oid: u64) -> AdapterResult<OrderInfo> {
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
