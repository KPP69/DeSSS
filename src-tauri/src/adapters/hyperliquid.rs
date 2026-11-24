//! Hyperliquid read-only adapter, shared by Hyperliquid mainnet and
//! Entropy (HIP-3, `dex = "io"`).
//!
//! - Market data: `wss://api.hyperliquid.xyz/ws`, `l2Book` subscription.
//!   Each frame carries the full book, so it is applied as a snapshot.
//! - Account (read-only): `POST https://api.hyperliquid.xyz/info`
//!   `clearinghouseState` — real equity, withdrawable margin, open
//!   positions and unrealised PnL. No order placement in this phase.
//! - Entropy reuses the same endpoints: market names carry the `io:`
//!   prefix and the REST call adds `"dex": "io"`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use super::book::BookState;
use super::types::{AccountLive, Balance, LivePosition, OrderRequest, Order, Position, Tick};
use super::{AdapterResult, ExchangeAdapter};
use crate::bus::{AppEvent, EventBus};

pub const HL_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub const HL_INFO_URL: &str = "https://api.hyperliquid.xyz/info";

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

/// Last REST snapshot, merged with WS status/latency for `AccountLive`.
#[derive(Clone, Default)]
struct Snapshot {
    equity_usd: f64,
    available_usd: f64,
    pnl_usd: f64,
    positions: Vec<LivePosition>,
}

pub struct HyperliquidAdapter {
    id: String,
    dex: Option<String>,
    address: String,
    account_id: String,
    http: reqwest::Client,
    bus: EventBus,
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    running: Arc<AtomicBool>,
    books: Mutex<HashMap<String, BookState>>,
    snapshot: Mutex<Snapshot>,
    status: Mutex<String>,
    last_error: Mutex<Option<String>>,
    last_frame: Mutex<Instant>,
    last_rtt: Mutex<Option<Duration>>,
}

impl HyperliquidAdapter {
    pub fn new(
        id: &str,
        dex: Option<&str>,
        address: String,
        account_id: String,
        bus: EventBus,
        live: Arc<RwLock<HashMap<String, AccountLive>>>,
    ) -> Self {
        let symbols = if dex.is_some() { vec!["io:BTC", "io:ETH"] } else { vec!["BTC", "ETH"] };
        let _ = symbols;
        Self {
            id: id.to_string(),
            dex: dex.map(|d| d.to_string()),
            address,
            account_id,
            http: reqwest::Client::builder().timeout(REST_TIMEOUT).build().unwrap_or_default(),
            bus,
            live,
            running: Arc::new(AtomicBool::new(false)),
            books: Mutex::new(HashMap::new()),
            snapshot: Mutex::new(Snapshot::default()),
            status: Mutex::new("connecting".to_string()),
            last_error: Mutex::new(None),
            last_frame: Mutex::new(Instant::now()),
            last_rtt: Mutex::new(None),
        }
    }

    fn symbols(&self) -> Vec<String> {
        if self.dex.is_some() {
            vec!["io:BTC".to_string(), "io:ETH".to_string()]
        } else {
            vec!["BTC".to_string(), "ETH".to_string()]
        }
    }

    /// WS subscription coin for a display symbol.
    fn coin_of(symbol: &str) -> &str {
        symbol
    }

    fn latency_ms(&self) -> Option<u64> {
        if let Some(rtt) = *self.last_rtt.lock().unwrap() {
            return Some(rtt.as_millis() as u64);
        }
        let stale = self.last_frame.lock().unwrap().elapsed().as_millis() as u64;
        Some(stale)
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
            equity_usd: snap.equity_usd,
            available_usd: snap.available_usd,
            pnl_usd: snap.pnl_usd,
            currency: "USDC".to_string(),
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

    /// One REST read of clearinghouseState. Returns (snapshot, balance).
    async fn fetch_clearinghouse(&self) -> Result<(Snapshot, Balance), String> {
        let mut body = serde_json::json!({
            "type": "clearinghouseState",
            "user": self.address,
        });
        if let Some(dex) = &self.dex {
            body["dex"] = Value::String(dex.clone());
        }
        let resp = self
            .http
            .post(HL_INFO_URL)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("hyperliquid info request failed: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("hyperliquid info returned HTTP {}", resp.status()));
        }
        let v: Value = resp.json().await.map_err(|e| format!("bad info response: {e}"))?;
        let margin = &v["marginSummary"];
        let equity = f64_from(&margin["accountValue"]);
        let withdrawable = f64_from(&v["withdrawable"]);
        let mut positions = Vec::new();
        let mut pnl = 0.0;
        if let Some(list) = v["assetPositions"].as_array() {
            for item in list {
                let p = &item["position"];
                if p.is_null() {
                    continue;
                }
                let upnl = f64_from(&p["unrealizedPnl"]);
                pnl += upnl;
                let szi = f64_from(&p["szi"]);
                positions.push(LivePosition {
                    symbol: p["coin"].as_str().unwrap_or("").to_string(),
                    side: if szi >= 0.0 { "long".to_string() } else { "short".to_string() },
                    qty: szi.abs(),
                    entry_price: f64_from(&p["entryPx"]),
                    mark_price: f64_from(&p["positionValue"]) / szi.abs().max(1e-12),
                    unrealized_pnl: upnl,
                });
            }
        }
        let snap = Snapshot { equity_usd: equity, available_usd: withdrawable, pnl_usd: pnl, positions };
        let balance = Balance {
            adapter_id: self.id.clone(),
            account_id: self.account_id.clone(),
            asset: "USDC".to_string(),
            free: withdrawable,
            locked: (equity - withdrawable).max(0.0),
            total: equity,
            equity_usd: equity,
            ts: now_ms(),
        };
        Ok((snap, balance))
    }

    async fn poll_account(&self) {
        match self.fetch_clearinghouse().await {
            Ok((snap, balance)) => {
                *self.snapshot.lock().unwrap() = snap;
                self.bus.publish(AppEvent::Balance(balance));
                self.publish_live();
            }
            Err(e) => {
                tracing::warn!("[hyperliquid] account poll failed: {e}");
            }
        }
    }

    fn handle_l2book(&self, coin: &str, levels: &Value) {
        let parse_side = |v: &Value| -> Vec<(f64, f64)> {
            v.as_array()
                .map(|arr| {
                    arr.iter()
                        .map(|lv| (f64_from(&lv["px"]), f64_from(&lv["sz"])))
                        .collect()
                })
                .unwrap_or_default()
        };
        let bids = levels.get(0).map(&parse_side).unwrap_or_default();
        let asks = levels.get(1).map(&parse_side).unwrap_or_default();
        let ts = now_ms();
        let mut books = self.books.lock().unwrap();
        let state = books.entry(coin.to_string()).or_insert_with(BookState::new);
        state.apply_snapshot(&bids, &asks);
        let book = state.to_book(&self.id, coin, BOOK_DEPTH, ts);
        let (best_bid, best_ask) = (
            book.bids.first().map(|l| l.0).unwrap_or(0.0),
            book.asks.first().map(|l| l.0).unwrap_or(0.0),
        );
        drop(books);
        self.bus.publish(AppEvent::Book(book));
        if best_bid > 0.0 && best_ask > 0.0 {
            self.bus.publish(AppEvent::Tick(Tick {
                adapter_id: self.id.clone(),
                symbol: coin.to_string(),
                bid: best_bid,
                ask: best_ask,
                last: (best_bid + best_ask) / 2.0,
                volume24h: 0.0,
                ts,
            }));
        }
    }

    async fn run_once(&self) -> Result<(), String> {
        let (mut ws, _) = tokio::time::timeout(WS_CONNECT_TIMEOUT, tokio_tungstenite::connect_async(HL_WS_URL))
            .await
            .map_err(|_| "hyperliquid ws connect timed out".to_string())
            .and_then(|r| r.map_err(|e| format!("hyperliquid ws connect failed: {e}")))?;
        for sym in self.symbols() {
            let sub = serde_json::json!({
                "method": "subscribe",
                "subscription": { "type": "l2Book", "coin": Self::coin_of(&sym) },
            });
            ws.send(Message::Text(sub.to_string().into()))
                .await
                .map_err(|e| format!("subscribe failed: {e}"))?;
        }
        self.set_status("connected", None);
        // Show real equity quickly, then keep polling on the interval.
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
                    let ping = serde_json::json!({"method": "ping"});
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
                                match v["channel"].as_str() {
                                    Some("l2Book") => {
                                        let coin = v["data"]["coin"].as_str().unwrap_or("").to_string();
                                        if !coin.is_empty() {
                                            self.handle_l2book(&coin, &v["data"]["levels"]);
                                        }
                                    }
                                    Some("pong") => {
                                        if let Some(sent) = ping_sent.take() {
                                            *self.last_rtt.lock().unwrap() = Some(sent.elapsed());
                                            self.publish_live();
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Some(Ok(Message::Close(_))) => return Err("hyperliquid ws closed".to_string()),
                        Some(Err(e)) => return Err(format!("hyperliquid ws error: {e}")),
                        None => return Err("hyperliquid ws stream ended".to_string()),
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
impl ExchangeAdapter for HyperliquidAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        if self.dex.is_some() { "Entropy" } else { "Hyperliquid" }
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
        let (_, balance) = self.fetch_clearinghouse().await.map_err(super::AdapterError::Other)?;
        Ok(vec![balance])
    }

    async fn get_positions(&self, _account_id: &str) -> AdapterResult<Vec<Position>> {
        let (snap, _) = self.fetch_clearinghouse().await.map_err(super::AdapterError::Other)?;
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
