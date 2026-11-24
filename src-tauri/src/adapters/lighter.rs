//! Lighter read-only adapter, shared by Lighter mainnet and Lighter on
//! Robinhood Chain (same REST/WS API, different base URLs and chain ID).
//!
//! Official endpoints (docs.lighter.xyz / apidocs.lighter.xyz):
//! - Mainnet: REST `https://mainnet.zklighter.elliot.ai`,
//!   WS `wss://mainnet.zklighter.elliot.ai/stream`, chain 304, USDC.
//! - Robinhood Chain: REST `https://api.rh.lighter.xyz`,
//!   WS `wss://api.rh.lighter.xyz/stream`, chain 466324, USDG.
//!
//! - Market data: WS `order_book/{market_id}`. The first frame after
//!   subscribe is a full snapshot (`subscribed/order_book`); later frames
//!   are deltas (`update/order_book`). Every delta's `offset` must be
//!   strictly greater than the previous one (and `begin_nonce` must match
//!   the previous `nonce`); on any gap the local book is cleared and the
//!   connection is re-established to pull a fresh snapshot.
//! - Keepalive: send `{"type":"ping"}` at least every 2 minutes; the
//!   server answers `{"type":"pong"}`. A server `ping` is answered with
//!   `pong` as well. No auth token is used: market data and the public
//!   account read are unauthenticated, so there is nothing to refresh.
//! - Account (read-only): `GET /api/v1/account?by=index&value={index}` —
//!   real collateral, available balance, unrealised PnL and positions.
//!   No order placement in this phase.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use super::book::BookState;
use super::types::{AccountLive, Balance, LivePosition, Order, OrderRequest, Position, Tick};
use super::{AdapterResult, ExchangeAdapter};
use crate::bus::{AppEvent, EventBus};

const REST_TIMEOUT: Duration = Duration::from_secs(10);
const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const PING_INTERVAL: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_secs(10);
const BOOK_DEPTH: usize = 20;

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn f64_from(v: &Value) -> f64 {
    match v {
        Value::String(s) => s.parse().unwrap_or(0.0),
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        _ => 0.0,
    }
}

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

    pub fn ws_url(&self) -> &'static str {
        match self {
            LighterNetwork::Mainnet => "wss://mainnet.zklighter.elliot.ai/stream",
            LighterNetwork::Robinhood => "wss://api.rh.lighter.xyz/stream",
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

/// Last REST snapshot, merged with WS status/latency for `AccountLive`.
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
    /// market_id -> local book.
    books: Mutex<HashMap<u64, BookState>>,
    /// symbol ("BTC"/"ETH") -> market_id, resolved from /api/v1/orderBooks.
    market_ids: Mutex<HashMap<String, u64>>,
    /// market_id -> symbol, for naming books/ticks.
    symbols: Mutex<HashMap<u64, String>>,
    /// market_id -> nonce of the last accepted frame (continuity check).
    last_nonce: Mutex<HashMap<u64, u64>>,
    snapshot: Mutex<Snapshot>,
    status: Mutex<String>,
    last_error: Mutex<Option<String>>,
    last_frame: Mutex<Instant>,
    last_rtt: Mutex<Option<Duration>>,
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
            http: super::new_http_client(),
            bus,
            live,
            running: Arc::new(AtomicBool::new(false)),
            books: Mutex::new(HashMap::new()),
            market_ids: Mutex::new(HashMap::new()),
            symbols: Mutex::new(HashMap::new()),
            last_nonce: Mutex::new(HashMap::new()),
            snapshot: Mutex::new(Snapshot::default()),
            status: Mutex::new("connecting".to_string()),
            last_error: Mutex::new(None),
            last_frame: Mutex::new(Instant::now()),
            last_rtt: Mutex::new(None),
        }
    }

    fn latency_ms(&self) -> Option<u64> {
        if let Some(rtt) = *self.last_rtt.lock().unwrap() {
            return Some(rtt.as_millis() as u64);
        }
        Some(self.last_frame.lock().unwrap().elapsed().as_millis() as u64)
    }

    fn publish_live(&self) {
        let snap = self.snapshot.lock().unwrap().clone();
        let status = self.status.lock().unwrap().clone();
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

    fn set_status(&self, status: &str, error: Option<String>) {
        *self.status.lock().unwrap() = status.to_string();
        *self.last_error.lock().unwrap() = error;
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

    fn publish_book(&self, market_id: u64) {
        let symbol = self.symbol_for(market_id);
        let ts = now_ms();
        let books = self.books.lock().unwrap();
        let Some(state) = books.get(&market_id) else {
            return;
        };
        let book = state.to_book(&self.id, &symbol, BOOK_DEPTH, ts);
        let (best_bid, best_ask) = (
            book.bids.first().map(|l| l.0).unwrap_or(0.0),
            book.asks.first().map(|l| l.0).unwrap_or(0.0),
        );
        drop(books);
        self.bus.publish(AppEvent::Book(book));
        if best_bid > 0.0 && best_ask > 0.0 {
            self.bus.publish(AppEvent::Tick(Tick {
                adapter_id: self.id.clone(),
                symbol,
                bid: best_bid,
                ask: best_ask,
                last: (best_bid + best_ask) / 2.0,
                volume24h: 0.0,
                ts,
            }));
        }
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
            .map_err(|e| format!("lighter orderBooks request failed: {e:?}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "lighter orderBooks returned HTTP {}",
                resp.status()
            ));
        }
        let text = resp
            .text()
            .await
            .map_err(|e| format!("lighter orderBooks read failed: {e:?}"))?;
        let v: Value = serde_json::from_str(&text).map_err(|e| {
            format!(
                "lighter orderBooks: invalid JSON ({e}); body: {}",
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
            .map_err(|e| format!("lighter account request failed: {e:?}"))?;
        if !resp.status().is_success() {
            return Err(format!("lighter account returned HTTP {}", resp.status()));
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| format!("bad account response: {e}"))?;
        let Some(acc) = v["accounts"].as_array().and_then(|a| a.first()) else {
            return Err(format!("lighter account {} not found", self.account_index));
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
        match self.fetch_account().await {
            Ok((snap, balance)) => {
                *self.snapshot.lock().unwrap() = snap;
                self.bus.publish(AppEvent::Balance(balance));
                self.publish_live();
            }
            Err(e) => {
                tracing::warn!("[lighter] account poll failed: {e}");
            }
        }
    }

    /// Handle one order_book frame. Returns Err on a sequence gap, in
    /// which case the caller drops the connection and resubscribes for a
    /// fresh snapshot.
    fn handle_book_frame(&self, msg: &Value) -> Result<(), String> {
        let channel = msg["channel"].as_str().unwrap_or("");
        let Some(market_id) = channel
            .strip_prefix("order_book:")
            .and_then(|s| s.parse::<u64>().ok())
        else {
            return Ok(());
        };
        let ob = &msg["order_book"];
        let parse_side = |v: &Value| -> Vec<(f64, f64)> {
            v.as_array()
                .map(|arr| {
                    arr.iter()
                        .map(|lv| (f64_from(&lv["price"]), f64_from(&lv["size"])))
                        .collect()
                })
                .unwrap_or_default()
        };
        let bids = parse_side(&ob["bids"]);
        let asks = parse_side(&ob["asks"]);
        let offset = u64_from(&ob["offset"]).or_else(|| u64_from(&msg["offset"]));
        let nonce = u64_from(&ob["nonce"]);
        let begin_nonce = u64_from(&ob["begin_nonce"]);
        let is_snapshot = msg["type"].as_str() == Some("subscribed/order_book");

        let mut books = self.books.lock().unwrap();
        let state = books.entry(market_id).or_insert_with(BookState::new);
        if is_snapshot {
            let off = offset.unwrap_or(0);
            state.apply_snapshot_with_offset(off, &bids, &asks);
            if let Some(n) = nonce {
                self.last_nonce.lock().unwrap().insert(market_id, n);
            }
        } else {
            // Continuity: offset must strictly increase; when the server
            // reports nonces, begin_nonce must match the previous nonce.
            if let Some(off) = offset {
                if let Err(gap) = state.apply_delta(off, &bids, &asks) {
                    state.clear();
                    return Err(format!(
                        "lighter order_book gap for market {market_id}: {gap}"
                    ));
                }
            }
            if let (Some(begin), Some(n)) = (begin_nonce, nonce) {
                let mut last = self.last_nonce.lock().unwrap();
                if let Some(prev) = last.get(&market_id) {
                    if begin != *prev {
                        state.clear();
                        return Err(format!(
                            "lighter order_book nonce gap for market {market_id}: begin_nonce {begin} != last nonce {prev}"
                        ));
                    }
                }
                last.insert(market_id, n);
            } else if let Some(n) = nonce {
                self.last_nonce.lock().unwrap().insert(market_id, n);
            }
        }
        drop(books);
        self.publish_book(market_id);
        Ok(())
    }

    async fn run_once(&self) -> Result<(), String> {
        self.fetch_markets().await?;
        {
            let mut books = self.books.lock().unwrap();
            books.clear();
            for mid in self.market_ids.lock().unwrap().values() {
                books.insert(*mid, BookState::new());
            }
        }
        self.last_nonce.lock().unwrap().clear();

        let (mut ws, _) = tokio::time::timeout(
            WS_CONNECT_TIMEOUT,
            tokio_tungstenite::connect_async(self.network.ws_url()),
        )
        .await
        .map_err(|_| "lighter ws connect timed out".to_string())
        .and_then(|r| r.map_err(|e| format!("lighter ws connect failed: {e}")))?;

        let market_ids: Vec<u64> = self.market_ids.lock().unwrap().values().copied().collect();
        for mid in &market_ids {
            let sub = serde_json::json!({
                "type": "subscribe",
                "channel": format!("order_book/{mid}"),
            });
            ws.send(Message::Text(sub.to_string().into()))
                .await
                .map_err(|e| format!("subscribe failed: {e}"))?;
        }
        self.set_status("connected", None);
        self.poll_account().await;

        let mut ping_i = tokio::time::interval(PING_INTERVAL);
        let mut poll_i = tokio::time::interval(POLL_INTERVAL);
        let mut stop_i = tokio::time::interval(Duration::from_secs(1));
        ping_i.tick().await;
        poll_i.tick().await;
        stop_i.tick().await;
        let mut ping_sent: Option<Instant> = None;

        loop {
            tokio::select! {
                _ = poll_i.tick() => { self.poll_account().await; }
                _ = ping_i.tick() => {
                    let ping = serde_json::json!({"type": "ping"});
                    if ws.send(Message::Text(ping.to_string().into())).await.is_err() {
                        return Err("ws send ping failed".to_string());
                    }
                    ping_sent = Some(Instant::now());
                }
                _ = stop_i.tick() => {}
                msg = ws.next() => {
                    match msg {
                        Some(Ok(Message::Text(t))) => {
                            *self.last_frame.lock().unwrap() = Instant::now();
                            let text: &str = t.as_ref();
                            if let Ok(v) = serde_json::from_str::<Value>(text) {
                                if v["type"].as_str() == Some("ping") {
                                    let pong = serde_json::json!({"type": "pong"});
                                    let _ = ws.send(Message::Text(pong.to_string().into())).await;
                                } else if v["type"].as_str() == Some("pong") {
                                    if let Some(sent) = ping_sent.take() {
                                        *self.last_rtt.lock().unwrap() = Some(sent.elapsed());
                                        self.publish_live();
                                    }
                                } else if v["type"].as_str().is_some_and(|t| t.ends_with("/order_book")) {
                                    if let Err(e) = self.handle_book_frame(&v) {
                                        return Err(e);
                                    }
                                } else if v.get("error").is_some() {
                                    tracing::warn!("[lighter] ws error message: {v}");
                                }
                            }
                        }
                        Some(Ok(Message::Close(_))) => return Err("lighter ws closed".to_string()),
                        Some(Err(e)) => return Err(format!("lighter ws error: {e}")),
                        None => return Err("lighter ws stream ended".to_string()),
                        _ => {}
                    }
                }
            }
            if !self.running.load(Ordering::SeqCst) {
                return Ok(());
            }
        }
    }

    async fn run_loop(&self) {
        self.set_status("connecting", None);
        let mut backoff: u64 = 1;
        while self.running.load(Ordering::SeqCst) {
            match self.run_once().await {
                Ok(()) => break,
                Err(e) => {
                    if !self.running.load(Ordering::SeqCst) {
                        break;
                    }
                    self.set_status("reconnecting", Some(e));
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(30);
                }
            }
        }
        self.set_status("disconnected", None);
    }
}

#[async_trait]
impl ExchangeAdapter for LighterAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        if self.network == LighterNetwork::Robinhood {
            "Lighter (Robinhood Chain)"
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
        Err(super::AdapterError::Other(
            "order placement is not supported in the read-only phase".to_string(),
        ))
    }

    async fn cancel_order(&self, _account_id: &str, _order_id: &str) -> AdapterResult<()> {
        Err(super::AdapterError::Other(
            "order cancellation is not supported in the read-only phase".to_string(),
        ))
    }

    async fn run(&self) {
        self.run_loop().await;
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
        assert_eq!(
            LighterNetwork::Mainnet.ws_url(),
            "wss://mainnet.zklighter.elliot.ai/stream"
        );
        assert_eq!(LighterNetwork::Mainnet.chain_id(), 304);
        assert_eq!(LighterNetwork::Mainnet.currency(), "USDC");
        assert_eq!(
            LighterNetwork::Robinhood.rest_base(),
            "https://api.rh.lighter.xyz"
        );
        assert_eq!(
            LighterNetwork::Robinhood.ws_url(),
            "wss://api.rh.lighter.xyz/stream"
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

    #[test]
    fn book_frame_gap_triggers_resync_error() {
        let (bus, live) = test_ctx();
        let a = LighterAdapter::new(
            "lighter",
            LighterNetwork::Mainnet,
            1,
            "acc-1".into(),
            bus,
            live,
        );
        let snap = serde_json::json!({
            "type": "subscribed/order_book",
            "channel": "order_book:0",
            "order_book": {
                "offset": 100, "nonce": 50, "begin_nonce": 0,
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "1"}]
            }
        });
        assert!(a.handle_book_frame(&snap).is_ok());
        // Stale offset: must fail so the caller clears + resubscribes.
        let stale = serde_json::json!({
            "type": "update/order_book",
            "channel": "order_book:0",
            "order_book": {
                "offset": 100, "nonce": 51, "begin_nonce": 50,
                "bids": [], "asks": []
            }
        });
        assert!(a.handle_book_frame(&stale).is_err());
        // Fresh offset + matching begin_nonce passes.
        let good = serde_json::json!({
            "type": "update/order_book",
            "channel": "order_book:0",
            "order_book": {
                "offset": 101, "nonce": 51, "begin_nonce": 50,
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        });
        // NOTE: the stale frame above cleared the book, so this delta applies
        // to an empty book with no offset baseline and is accepted.
        assert!(a.handle_book_frame(&good).is_ok());
    }

    fn test_ctx() -> (EventBus, Arc<RwLock<HashMap<String, AccountLive>>>) {
        (EventBus::new(16), Arc::new(RwLock::new(HashMap::new())))
    }
}
