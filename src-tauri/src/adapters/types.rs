use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tick {
    pub adapter_id: String,
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
    pub volume24h: f64,
    pub ts: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Balance {
    pub adapter_id: String,
    pub account_id: String,
    pub asset: String,
    pub free: f64,
    pub locked: f64,
    pub total: f64,
    pub equity_usd: f64,
    pub ts: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub adapter_id: String,
    pub account_id: String,
    pub symbol: String,
    pub side: String,
    pub qty: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderRequest {
    pub adapter_id: String,
    pub account_id: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub qty: f64,
    pub price: Option<f64>,
    pub client_order_id: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    pub id: String,
    pub adapter_id: String,
    pub account_id: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub qty: f64,
    pub price: Option<f64>,
    pub status: String,
    pub ts: i64,
}

/// One price level of an order book: [price, quantity].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceLevel(pub f64, pub f64);

/// Full order-book snapshot for one market (top levels only, best first).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBook {
    pub adapter_id: String,
    pub symbol: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub ts: i64,
}

/// One open position in the live-account snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LivePosition {
    pub symbol: String,
    pub side: String,
    pub qty: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
}

/// Live read-only state of one connected real account, pushed on every
/// REST poll and on every connection state change. `currency` is the
/// settlement currency reported by the exchange (e.g. USDC).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountLive {
    pub account_id: String,
    pub adapter_id: String,
    /// "connecting" | "connected" | "reconnecting" | "error" | "disconnected"
    pub status: String,
    /// Last measured round-trip (ping/pong) or frame staleness in ms, if known.
    pub latency_ms: Option<u64>,
    pub equity_usd: f64,
    pub available_usd: f64,
    pub pnl_usd: f64,
    pub currency: String,
    pub positions: Vec<LivePosition>,
    pub error: Option<String>,
    pub ts: i64,
}
