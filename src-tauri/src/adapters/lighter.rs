//! Lighter read-only adapter, shared by Lighter mainnet and Lighter RH
//! (same REST API, different base URLs and chain ID).
//!
//! Official endpoints (docs.lighter.xyz / apidocs.lighter.xyz):
//! - Mainnet: REST `https://mainnet.zklighter.elliot.ai`, chain 304, USDC.
//! - Lighter RH: REST `https://api.rh.lighter.xyz`, chain 466324, USDG.
//!
//! - Market data: none. The adapter opens no WS connection at all —
//!   public market data comes from the on-demand catalog.
//! - Account (read-only): `GET /api/v1/account?by=index&value={index}` —
//!   real collateral, available balance, unrealised PnL and positions.
//!   The market list (`/api/v1/orderBooks`) is fetched once at startup
//!   only to name positions that carry a market id instead of a symbol.
//!   No order placement in this phase.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;

use super::types::{AccountLive, Balance, LivePosition, Order, OrderRequest, Position};
use super::{f64_from, now_ms, AdapterResult, ExchangeAdapter};
use crate::bus::{AppEvent, EventBus};

const POLL_INTERVAL: Duration = Duration::from_secs(10);

fn u64_from(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Parse the market list from a `GET /api/v1/orderBooks` response, lenient
/// about the wire shape: the outer list is `order_books` (snake_case) on
/// the live API, with `orderBooks` accepted as a fallback. Only active
/// perpetual markets are kept (`market_type == "perp"` when present,
/// `status == "active"` when present). Returns symbol -> market_id.
fn parse_order_books(v: &Value) -> Result<Vec<(String, u64)>, String> {
    let list = v
        .get("order_books")
        .or_else(|| v.get("orderBooks"))
        .and_then(|l| l.as_array())
        .ok_or_else(|| {
            let keys: Vec<&str> = v
                .as_object()
                .map(|o| o.keys().map(|k| k.as_str()).collect())
                .unwrap_or_default();
            format!("no order_books/orderBooks array in response (top-level keys: {keys:?})")
        })?;
    let mut out = Vec::new();
    for m in list {
        if let Some(t) = m.get("market_type").and_then(|t| t.as_str()) {
            if t != "perp" {
                continue;
            }
        }
        if let Some(s) = m.get("status").and_then(|s| s.as_str()) {
            if s != "active" {
                continue;
            }
        }
        let Some(sym) = m.get("symbol").and_then(|s| s.as_str()) else {
            continue;
        };
        let Some(mid) = m.get("market_id").and_then(u64_from) else {
            continue;
        };
        // Perpetual markets are named "BTC"/"ETH"; spot pairs ("BTC/USDC")
        // never match the exact names below.
        if sym == "BTC" || sym == "ETH" {
            out.push((sym.to_string(), mid));
        }
    }
    Ok(out)
}

/// First `n` characters of a response body (char-boundary safe), for error
/// messages that would otherwise hide the real payload.
fn body_snippet(text: &str, n: usize) -> &str {
    match text.char_indices().nth(n) {
        Some((i, _)) => &text[..i],
        None => text,
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LighterNetwork {
    Mainnet,
    Robinhood,
}

impl LighterNetwork {
    pub fn rest_base(&self) -> &'static str {
        match self {
            LighterNetwork::Mainnet => "https://mainnet.zklighter.elliot.ai",
            LighterNetwork::Robinhood => "https://api.rh.lighter.xyz",
        }
    }

    /// Signing chain id (recorded for completeness; the read-only phase
    /// never signs anything).
    pub fn chain_id(&self) -> u64 {
        match self {
            LighterNetwork::Mainnet => 304,
            LighterNetwork::Robinhood => 466324,
        }
    }

    pub fn currency(&self) -> &'static str {
        match self {
            LighterNetwork::Mainnet => "USDC",
            LighterNetwork::Robinhood => "USDG",
        }
    }
}

/// Last REST snapshot, published as `AccountLive`.
#[derive(Clone, Default)]
struct Snapshot {
    equity: f64,
    available: f64,
    pnl: f64,
    positions: Vec<LivePosition>,
}

pub struct LighterAdapter {
    id: String,
    network: LighterNetwork,
    account_index: u64,
    account_id: String,
    http: reqwest::Client,
    bus: EventBus,
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    running: Arc<AtomicBool>,
    /// symbol ("BTC"/"ETH") -> market_id, resolved from /api/v1/orderBooks.
    market_ids: Mutex<HashMap<String, u64>>,
    /// market_id -> symbol, for naming positions that carry only a
    /// market id.
    symbols: Mutex<HashMap<u64, String>>,
    snapshot: Mutex<Snapshot>,
    rest_ok: AtomicBool,
    last_error: Mutex<Option<String>>,
    last_rest_rtt: Mutex<Option<Duration>>,
}

impl LighterAdapter {
    pub fn new(
        id: &str,
        network: LighterNetwork,
        account_index: u64,
        account_id: String,
        bus: EventBus,
        live: Arc<RwLock<HashMap<String, AccountLive>>>,
    ) -> Self {
        Self {
            id: id.to_string(),
            network,
            account_index,
            account_id,
            http: crate::net::shared_http_client(),
            bus,
            live,
            running: Arc::new(AtomicBool::new(false)),
            market_ids: Mutex::new(HashMap::new()),
            symbols: Mutex::new(HashMap::new()),
            snapshot: Mutex::new(Snapshot::default()),
            rest_ok: AtomicBool::new(false),
            last_error: Mutex::new(None),
            last_rest_rtt: Mutex::new(None),
        }
    }

    /// Latency is always one measured round-trip: the last REST request
    /// time. Frame staleness and wall-clock subtraction are never used —
    /// a stale frame timestamp is what made latency run away to 1.5M ms.
    fn latency_ms(&self) -> Option<u64> {
        self.last_rest_rtt
            .lock()
            .unwrap()
            .map(|r| r.as_millis() as u64)
    }

    /// One console log line for the in-app drawer.
    fn log(&self, level: &str, message: String) {
        self.bus.log(level, self.name(), message);
    }

    fn publish_live(&self) {
        let snap = self.snapshot.lock().unwrap().clone();
        let status = super::live_status(
            self.rest_ok.load(Ordering::SeqCst),
            self.last_error.lock().unwrap().is_some(),
        )
        .to_string();
        let error = self.last_error.lock().unwrap().clone();
        let live = AccountLive {
            account_id: self.account_id.clone(),
            adapter_id: self.id.clone(),
            status,
            latency_ms: self.latency_ms(),
            equity_usd: snap.equity,
            available_usd: snap.available,
            pnl_usd: snap.pnl,
            currency: self.network.currency().to_string(),
            positions: snap.positions,
            error,
            ts: now_ms(),
        };
        if let Ok(mut map) = self.live.write() {
            map.insert(self.account_id.clone(), live.clone());
        }
        self.bus.publish(AppEvent::AccountLive(live));
    }

    /// REST state changed: record it and republish.
    fn set_rest(&self, ok: bool, rtt: Option<Duration>) {
        self.rest_ok.store(ok, Ordering::SeqCst);
        if let Some(r) = rtt {
            *self.last_rest_rtt.lock().unwrap() = Some(r);
        }
        self.publish_live();
    }

    fn symbol_for(&self, market_id: u64) -> String {
        self.symbols
            .lock()
            .unwrap()
            .get(&market_id)
            .cloned()
            .unwrap_or_else(|| format!("market-{market_id}"))
    }

    /// Resolve BTC/ETH market ids from the public market list. Market ids
    /// differ between deployments and over time, so they are never hardcoded.
    async fn fetch_markets(&self) -> Result<(), String> {
        let url = format!("{}/api/v1/orderBooks", self.network.rest_base());
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Lighter 盘口请求失败：{e:?}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "lighter orderBooks returned HTTP {}",
                resp.status()
            ));
        }
        let text = resp
            .text()
            .await
            .map_err(|e| format!("Lighter 盘口读取失败：{e:?}"))?;
        let v: Value = serde_json::from_str(&text).map_err(|e| {
            format!(
                "Lighter 盘口响应不是有效 JSON（{e}）；内容：{}",
                body_snippet(&text, 500)
            )
        })?;
        let parsed = parse_order_books(&v).map_err(|e| {
            format!(
                "lighter orderBooks: {e}; body: {}",
                body_snippet(&text, 500)
            )
        })?;
        let mut ids = HashMap::new();
        let mut names = HashMap::new();
        for (sym, mid) in parsed {
            if !ids.contains_key(&sym) {
                names.insert(mid, sym.clone());
                ids.insert(sym, mid);
            }
        }
        if !ids.contains_key("BTC") || !ids.contains_key("ETH") {
            return Err(format!(
                "lighter market list has no active BTC/ETH perps (got {ids:?}); body: {}",
                body_snippet(&text, 500)
            ));
        }
        *self.market_ids.lock().unwrap() = ids;
        *self.symbols.lock().unwrap() = names;
        Ok(())
    }

    /// One REST read of the public account endpoint.
    async fn fetch_account(&self) -> Result<(Snapshot, Balance), String> {
        let url = format!(
            "{}/api/v1/account?by=index&value={}",
            self.network.rest_base(),
            self.account_index
        );
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Lighter 账户请求失败：{e:?}"))?;
        if !resp.status().is_success() {
            return Err(format!("lighter account returned HTTP {}", resp.status()));
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| format!("账户响应解析失败：{e}"))?;
        let Some(acc) = v["accounts"].as_array().and_then(|a| a.first()) else {
            return Err(format!("Lighter 账户 {} 不存在", self.account_index));
        };
        let equity = f64_from(&acc["total_asset_value"]);
        let available = f64_from(&acc["available_balance"]);
        let pnl = f64_from(&acc["unrealized_pnl"]);
        let mut positions = Vec::new();
        if let Some(list) = acc["positions"].as_array() {
            for p in list {
                let qty_raw = f64_from(&p["position"]);
                if qty_raw == 0.0 {
                    continue;
                }
                let symbol = p["symbol"]
                    .as_str()
                    .map(|s| s.to_string())
                    .or_else(|| u64_from(&p["market_id"]).map(|id| self.symbol_for(id)))
                    .unwrap_or_default();
                let sign = p["sign"]
                    .as_i64()
                    .unwrap_or(if qty_raw >= 0.0 { 1 } else { -1 });
                let qty = qty_raw.abs();
                let entry = f64_from(&p["avg_entry_price"]);
                let value = f64_from(&p["position_value"]);
                let mark = if qty > 0.0 && value != 0.0 {
                    value / qty
                } else {
                    entry
                };
                positions.push(LivePosition {
                    symbol,
                    side: if sign >= 0 {
                        "long".to_string()
                    } else {
                        "short".to_string()
                    },
                    qty,
                    entry_price: entry,
                    mark_price: mark,
                    unrealized_pnl: f64_from(&p["unrealized_pnl"]),
                });
            }
        }
        let snap = Snapshot {
            equity,
            available,
            pnl,
            positions,
        };
        let currency = self.network.currency().to_string();
        let balance = Balance {
            adapter_id: self.id.clone(),
            account_id: self.account_id.clone(),
            asset: currency,
            free: available,
            locked: (equity - available).max(0.0),
            total: equity,
            equity_usd: equity,
            ts: now_ms(),
        };
        Ok((snap, balance))
    }

    async fn poll_account(&self) {
        let started = Instant::now();
        match self.fetch_account().await {
            Ok((snap, balance)) => {
                *self.snapshot.lock().unwrap() = snap;
                self.bus.publish(AppEvent::Balance(balance));
                self.set_rest(true, Some(started.elapsed()));
            }
            Err(e) => {
                self.log("error", format!("账户轮询失败：{e}"));
                self.set_rest(false, None);
            }
        }
    }

    /// REST poll loop: as long as REST works, equity and positions keep
    /// showing. The market list is resolved once up front so positions that carry only
    /// a market id can be named; a failure there only warns.
    async fn rest_loop(&self) {
        if let Err(e) = self.fetch_markets().await {
            self.log(
                "warn",
                format!("市场列表读取失败，持仓可能显示 market-id：{e}"),
            );
        }
        self.poll_account().await;
        let mut tick = tokio::time::interval(POLL_INTERVAL);
        tick.tick().await;
        while self.running.load(Ordering::SeqCst) {
            tick.tick().await;
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            self.poll_account().await;
        }
    }
}

#[async_trait]
impl ExchangeAdapter for LighterAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        if self.network == LighterNetwork::Robinhood {
            "Lighter RH"
        } else {
            "Lighter"
        }
    }

    async fn connect(&self) -> AdapterResult<()> {
        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn disconnect(&self) -> AdapterResult<()> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn subscribe_ticks(&self, _symbols: Vec<String>) -> AdapterResult<()> {
        Ok(())
    }
    async fn unsubscribe_ticks(&self, _symbols: Vec<String>) -> AdapterResult<()> {
        Ok(())
    }

    async fn get_balances(&self, _account_id: &str) -> AdapterResult<Vec<Balance>> {
        let (_, balance) = self
            .fetch_account()
            .await
            .map_err(super::AdapterError::Other)?;
        Ok(vec![balance])
    }

    async fn get_positions(&self, _account_id: &str) -> AdapterResult<Vec<Position>> {
        let (snap, _) = self
            .fetch_account()
            .await
            .map_err(super::AdapterError::Other)?;
        Ok(snap
            .positions
            .iter()
            .map(|p| Position {
                adapter_id: self.id.clone(),
                account_id: self.account_id.clone(),
                symbol: p.symbol.clone(),
                side: p.side.clone(),
                qty: p.qty,
                entry_price: p.entry_price,
                mark_price: p.mark_price,
                unrealized_pnl: p.unrealized_pnl,
            })
            .collect())
    }

    async fn get_open_orders(&self, _account_id: &str) -> AdapterResult<Vec<Order>> {
        Ok(vec![])
    }

    async fn place_order(&self, _req: OrderRequest) -> AdapterResult<Order> {
        Err(super::AdapterError::Other("只读阶段不支持下单".to_string()))
    }

    async fn cancel_order(&self, _account_id: &str, _order_id: &str) -> AdapterResult<()> {
        Err(super::AdapterError::Other("只读阶段不支持撤单".to_string()))
    }

    async fn run(&self) {
        // Account sync is REST polling only: no market WS is opened for
        // an account (public market data comes from the on-demand
        // catalog, never from a per-account connection).
        self.rest_loop().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_endpoints_are_distinct() {
        assert_eq!(
            LighterNetwork::Mainnet.rest_base(),
            "https://mainnet.zklighter.elliot.ai"
        );
        assert_eq!(LighterNetwork::Mainnet.chain_id(), 304);
        assert_eq!(LighterNetwork::Mainnet.currency(), "USDC");
        assert_eq!(
            LighterNetwork::Robinhood.rest_base(),
            "https://api.rh.lighter.xyz"
        );
        assert_eq!(LighterNetwork::Robinhood.chain_id(), 466324);
        assert_eq!(LighterNetwork::Robinhood.currency(), "USDG");
    }

    #[test]
    fn order_books_parses_snake_case_and_filters_perps() {
        let v = serde_json::json!({
            "code": 200,
            "order_books": [
                {"symbol": "BTC", "market_id": 1, "market_type": "perp", "status": "active"},
                {"symbol": "ETH", "market_id": 0, "market_type": "perp", "status": "active"},
                {"symbol": "BTC/USDC", "market_id": 2049, "market_type": "spot", "status": "active"},
                {"symbol": "SOL", "market_id": 2, "market_type": "perp", "status": "inactive"}
            ]
        });
        let parsed = parse_order_books(&v).expect("parse");
        assert_eq!(parsed.len(), 2);
        assert!(parsed.contains(&("BTC".to_string(), 1)));
        assert!(parsed.contains(&("ETH".to_string(), 0)));
    }

    #[test]
    fn order_books_accepts_camel_case_fallback_and_reports_keys() {
        let v = serde_json::json!({
            "orderBooks": [
                {"symbol": "BTC", "market_id": 1},
                {"symbol": "ETH", "market_id": 0}
            ]
        });
        assert_eq!(parse_order_books(&v).expect("parse").len(), 2);
        let bad = serde_json::json!({"code": 200, "total": 0});
        let err = parse_order_books(&bad).unwrap_err();
        assert!(
            err.contains("order_books"),
            "error must name the expected fields: {err}"
        );
        assert!(
            err.contains("code"),
            "error must list the keys present: {err}"
        );
    }
}
