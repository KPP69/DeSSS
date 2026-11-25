//! Arcus read-only adapter.
//!
//! Official endpoints (docs.arcus.xyz/api-reference, updated 2026-11-12):
//! - Market list: `GET https://api.arcus.xyz/v1/markets` — the top 2
//!   active perpetual markets by 24h notional volume are subscribed
//!   (never hardcoded IDs; live values put BTC-USD / ETH-USD on top).
//! - Market data: `wss://api.arcus.xyz/v1/ws`,
//!   `{"type":"subscribe","channel":"l2Orderbook","id":"BTC-USD",
//!   "nLevels":5}` and the `bbo` channel. `l2Orderbook` frames are full
//!   snapshots (re-broadcast periodically), so they are applied with
//!   `apply_snapshot`; BBO frames refresh the best bid/ask tick.
//! - Account (read-only, no auth): `GET /v1/account?address=0x…`
//!   `&accountIndex=0` — real equity, free collateral and positions.
//!   There is no total-PnL field: PnL is derived as
//!   `equity - netDeposits` (all-time PnL, same basis as /portfolio).
//!   Settlement asset is USDG. The account form has no subaccount field,
//!   so only index 0 is reachable. The signing key is never read here:
//!   account reads need only the wallet address.

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
use super::{f64_from, now_ms, AdapterResult, ExchangeAdapter};
use crate::bus::{AppEvent, EventBus};

pub const ARCUS_REST_URL: &str = "https://api.arcus.xyz";
pub const ARCUS_WS_URL: &str = "wss://api.arcus.xyz/v1/ws";

const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const PING_INTERVAL: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_secs(10);
const BOOK_DEPTH: usize = 20;
const MARKET_COUNT: usize = 2;

/// Rank the market list by 24h notional volume and take the top N active
/// perpetual display names (`"BTC-USD"` …). Market IDs are never hardcoded.
fn parse_markets(v: &Value, n: usize) -> Result<Vec<String>, String> {
    let list = v["markets"]
        .as_array()
        .ok_or_else(|| "no markets array in /v1/markets response".to_string())?;
    let mut ranked: Vec<(f64, String)> = list
        .iter()
        .filter(|m| m["status"].as_str().map(|s| s == "ONLINE").unwrap_or(true))
        .filter(|m| {
            m["type"]
                .as_str()
                .map(|t| t == "PERPETUAL")
                .unwrap_or(true)
        })
        .filter_map(|m| {
            let name = m["marketDisplayName"].as_str()?.to_string();
            Some((f64_from(&m["volume24hNotional"]), name))
        })
        .collect();
    ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let out: Vec<String> = ranked.into_iter().take(n).map(|(_, name)| name).collect();
    if out.is_empty() {
        return Err("no active perpetual markets in /v1/markets response".to_string());
    }
    Ok(out)
}

/// Side of one position: the `side` literal when it parses
/// (case-insensitive `LONG` / `SHORT`), else the sign of `size`.
fn parse_side(side: &Value, size: f64) -> &'static str {
    match side.as_str().map(|s| s.to_ascii_lowercase()) {
        Some(s) if s == "long" => "long",
        Some(s) if s == "short" => "short",
        _ => {
            if size >= 0.0 {
                "long"
            } else {
                "short"
            }
        }
    }
}

/// Parse one `Position` object (shared by `/v1/account`,
/// `/v1/positions` and the WS `positions` snapshot).
fn parse_position(p: &Value) -> Option<LivePosition> {
    let size = f64_from(&p["size"]);
    if size == 0.0 {
        return None;
    }
    Some(LivePosition {
        symbol: p["marketDisplayName"].as_str().unwrap_or("").to_string(),
        side: parse_side(&p["side"], size).to_string(),
        qty: size.abs(),
        entry_price: f64_from(&p["averageEntryPrice"]),
        mark_price: f64_from(&p["markPx"]),
        unrealized_pnl: f64_from(&p["unrealizedPnl"]),
    })
}

/// Parse a `/v1/account` payload into
/// (equity, available, pnl, positions). PnL is derived: there is no
/// total-PnL field on the endpoint.
fn parse_account(v: &Value) -> Result<(f64, f64, f64, Vec<LivePosition>), String> {
    if v.get("equity").is_none() {
        return Err("账户数据格式异常（缺少 equity）".to_string());
    }
    let equity = f64_from(&v["equity"]);
    let available = f64_from(&v["freeCollateral"]);
    let pnl = equity - f64_from(&v["netDeposits"]);
    let mut positions = Vec::new();
    if let Some(map) = v["positions"].as_object() {
        for p in map.values() {
            if let Some(pos) = parse_position(p) {
                positions.push(pos);
            }
        }
    }
    Ok((equity, available, pnl, positions))
}

/// Parse one `l2Orderbook` contents object into (bids, asks).
fn parse_book(contents: &Value) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
    let side = |v: &Value| -> Vec<(f64, f64)> {
        v.as_array()
            .map(|arr| {
                arr.iter()
                    .map(|lv| (f64_from(&lv[0]), f64_from(&lv[1])))
                    .collect()
            })
            .unwrap_or_default()
    };
    (side(&contents["bids"]), side(&contents["asks"]))
}

/// Parse one `bbo` contents object into (best bid, best ask), each
/// (price, qty). Either side may be null when the book is empty.
fn parse_bbo(contents: &Value) -> (Option<(f64, f64)>, Option<(f64, f64)>) {
    let one = |v: &Value| -> Option<(f64, f64)> {
        if v.is_null() {
            return None;
        }
        Some((f64_from(&v["price"]), f64_from(&v["size"])))
    };
    (one(&contents["bestBid"]), one(&contents["bestAsk"]))
}

/// Last REST snapshot, merged with WS status/latency for `AccountLive`.
#[derive(Clone, Default)]
struct Snapshot {
    equity_usd: f64,
    available_usd: f64,
    pnl_usd: f64,
    positions: Vec<LivePosition>,
}

pub struct ArcusAdapter {
    id: String,
    address: String,
    account_id: String,
    http: reqwest::Client,
    bus: EventBus,
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    running: Arc<AtomicBool>,
    books: Mutex<HashMap<String, BookState>>,
    /// Resolved market display names (top 2 by volume). Empty until the
    /// first market-list fetch.
    resolved_markets: Mutex<Vec<String>>,
    snapshot: Mutex<Snapshot>,
    ws_up: AtomicBool,
    rest_ok: AtomicBool,
    last_error: Mutex<Option<String>>,
    last_rtt: Mutex<Option<Duration>>,
    last_rest_rtt: Mutex<Option<Duration>>,
}

impl ArcusAdapter {
    pub fn new(
        id: &str,
        address: String,
        account_id: String,
        bus: EventBus,
        live: Arc<RwLock<HashMap<String, AccountLive>>>,
    ) -> Self {
        Self {
            id: id.to_string(),
            address,
            account_id,
            http: crate::net::new_http_client(),
            bus,
            live,
            running: Arc::new(AtomicBool::new(false)),
            books: Mutex::new(HashMap::new()),
            resolved_markets: Mutex::new(Vec::new()),
            snapshot: Mutex::new(Snapshot::default()),
            ws_up: AtomicBool::new(false),
            rest_ok: AtomicBool::new(false),
            last_error: Mutex::new(None),
            last_rtt: Mutex::new(None),
            last_rest_rtt: Mutex::new(None),
        }
    }

    fn markets(&self) -> Vec<String> {
        self.resolved_markets.lock().unwrap().clone()
    }

    /// Latency is always one measured round-trip: the WS ping -> pong RTT
    /// while the WS is up, else the last REST request time.
    fn latency_ms(&self) -> Option<u64> {
        if self.ws_up.load(Ordering::SeqCst) {
            return self.last_rtt.lock().unwrap().map(|r| r.as_millis() as u64);
        }
        self.last_rest_rtt
            .lock()
            .unwrap()
            .map(|r| r.as_millis() as u64)
    }

    fn log(&self, level: &str, message: String) {
        self.bus.log(level, self.name(), message);
    }

    fn publish_live(&self) {
        let snap = self.snapshot.lock().unwrap().clone();
        let status = super::live_status(
            self.ws_up.load(Ordering::SeqCst),
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
            equity_usd: snap.equity_usd,
            available_usd: snap.available_usd,
            pnl_usd: snap.pnl_usd,
            currency: "USDG".to_string(),
            positions: snap.positions,
            error,
            ts: now_ms(),
        };
        if let Ok(mut map) = self.live.write() {
            map.insert(self.account_id.clone(), live.clone());
        }
        self.bus.publish(AppEvent::AccountLive(live));
    }

    fn set_ws(&self, up: bool, error: Option<String>) {
        self.ws_up.store(up, Ordering::SeqCst);
        if up {
            *self.last_error.lock().unwrap() = None;
        } else if let Some(e) = error {
            *self.last_error.lock().unwrap() = Some(e);
        }
        self.publish_live();
    }

    fn set_rest(&self, ok: bool, rtt: Option<Duration>) {
        self.rest_ok.store(ok, Ordering::SeqCst);
        if let Some(r) = rtt {
            *self.last_rest_rtt.lock().unwrap() = Some(r);
        }
        self.publish_live();
    }

    /// Fetch the market list and resolve the top 2 active perpetuals.
    async fn resolve_markets(&self) -> Result<(), String> {
        let url = format!("{ARCUS_REST_URL}/v1/markets");
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Arcus 市场列表请求失败：{e:?}"))?;
        if !resp.status().is_success() {
            return Err(format!("Arcus 市场列表接口返回 HTTP {}", resp.status()));
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| format!("市场列表响应解析失败：{e}"))?;
        let markets = parse_markets(&v, MARKET_COUNT)?;
        self.log("info", format!("已选出行情市场：{}", markets.join(", ")));
        *self.resolved_markets.lock().unwrap() = markets;
        Ok(())
    }

    /// One REST read of the account (no auth — address only).
    async fn fetch_account_state(&self) -> Result<(Snapshot, Balance), String> {
        let url = format!(
            "{ARCUS_REST_URL}/v1/account?address={}&accountIndex=0",
            self.address
        );
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Arcus 账户请求失败：{e:?}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            let snippet: String = body.chars().take(200).collect();
            return Err(format!("Arcus 账户接口返回 HTTP {status}：{snippet}"));
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| format!("账户响应解析失败：{e}"))?;
        let (equity, available, pnl, positions) = parse_account(&v)?;
        let snap = Snapshot {
            equity_usd: equity,
            available_usd: available,
            pnl_usd: pnl,
            positions,
        };
        let balance = Balance {
            adapter_id: self.id.clone(),
            account_id: self.account_id.clone(),
            asset: "USDG".to_string(),
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
        match self.fetch_account_state().await {
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

    /// REST poll loop, independent of the market WS.
    async fn rest_loop(&self) {
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

    fn handle_book(&self, market: &str, contents: &Value) {
        let (bids, asks) = parse_book(contents);
        let ts = now_ms();
        let mut books = self.books.lock().unwrap();
        let state = books.entry(market.to_string()).or_insert_with(BookState::new);
        state.apply_snapshot(&bids, &asks);
        let book = state.to_book(&self.id, market, BOOK_DEPTH, ts);
        let (best_bid, best_ask) = (
            book.bids.first().map(|l| l.0).unwrap_or(0.0),
            book.asks.first().map(|l| l.0).unwrap_or(0.0),
        );
        drop(books);
        self.bus.publish(AppEvent::Book(book));
        if best_bid > 0.0 && best_ask > 0.0 {
            self.bus.publish(AppEvent::Tick(Tick {
                adapter_id: self.id.clone(),
                symbol: market.to_string(),
                bid: best_bid,
                ask: best_ask,
                last: (best_bid + best_ask) / 2.0,
                volume24h: 0.0,
                ts,
            }));
        }
    }

    fn handle_bbo(&self, market: &str, contents: &Value) {
        let (bid, ask) = parse_bbo(contents);
        if let (Some((b, _)), Some((a, _))) = (bid, ask) {
            if b > 0.0 && a > 0.0 {
                self.bus.publish(AppEvent::Tick(Tick {
                    adapter_id: self.id.clone(),
                    symbol: market.to_string(),
                    bid: b,
                    ask: a,
                    last: (b + a) / 2.0,
                    volume24h: 0.0,
                    ts: now_ms(),
                }));
            }
        }
    }

    /// One WS session: resolve markets first, connect, subscribe to
    /// `l2Orderbook` (nLevels 5) + `bbo` per market, then pump frames.
    async fn ws_once(&self) -> Result<(), String> {
        self.resolve_markets().await?;
        let (mut ws, _) =
            crate::net::ws::connect_ws(ARCUS_WS_URL, WS_CONNECT_TIMEOUT, &self.bus).await?;
        for market in self.markets() {
            for channel in ["l2Orderbook", "bbo"] {
                let mut sub = serde_json::json!({
                    "type": "subscribe",
                    "channel": channel,
                    "id": market,
                });
                if channel == "l2Orderbook" {
                    sub["nLevels"] = Value::from(5);
                }
                ws.send(Message::Text(sub.to_string().into()))
                    .await
                    .map_err(|e| format!("订阅失败：{e}"))?;
            }
        }
        self.set_ws(true, None);
        self.log(
            "info",
            format!("行情已连接，已订阅 {}", self.markets().join(", ")),
        );

        let mut ping_i = tokio::time::interval(PING_INTERVAL);
        let mut stop_i = tokio::time::interval(Duration::from_secs(1));
        ping_i.tick().await;
        stop_i.tick().await;
        let mut ping_sent: Option<Instant> = None;

        loop {
            tokio::select! {
                _ = ping_i.tick() => {
                    if ws.send(Message::Ping(vec![].into())).await.is_err() {
                        return Err("行情心跳发送失败".to_string());
                    }
                    ping_sent = Some(Instant::now());
                }
                _ = stop_i.tick() => {}
                msg = ws.next() => {
                    match msg {
                        Some(Ok(Message::Text(t))) => {
                            let text: &str = t.as_ref();
                            match serde_json::from_str::<Value>(text) {
                                Ok(v) => {
                                    // Parse by channel + contents shape,
                                    // not by the envelope `type`.
                                    let channel = v["channel"].as_str().unwrap_or("");
                                    let market = v["id"].as_str().unwrap_or("").to_string();
                                    let contents = &v["contents"];
                                    if !market.is_empty() && !contents.is_null() {
                                        match channel {
                                            "l2Orderbook" => self.handle_book(&market, contents),
                                            "bbo" => self.handle_bbo(&market, contents),
                                            _ => {}
                                        }
                                    }
                                }
                                Err(e) => self.log(
                                    "error",
                                    format!(
                                        "行情数据解析失败：{e}（前 200 字符：{}）",
                                        text.chars().take(200).collect::<String>().as_str()
                                    ),
                                ),
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {
                            if let Some(sent) = ping_sent.take() {
                                *self.last_rtt.lock().unwrap() = Some(sent.elapsed());
                                self.publish_live();
                            }
                        }
                        Some(Ok(Message::Close(_))) => return Err("Arcus 行情连接已关闭".to_string()),
                        Some(Err(e)) => return Err(format!("Arcus 行情出错：{e}")),
                        None => return Err("Arcus 行情流已结束".to_string()),
                        _ => {}
                    }
                }
            }
            if !self.running.load(Ordering::SeqCst) {
                return Ok(());
            }
        }
    }

    /// WS reconnect loop with a ladder backoff (5s -> 15s -> 30s cap).
    async fn ws_loop(&self) {
        let mut attempt: usize = 0;
        while self.running.load(Ordering::SeqCst) {
            let started = Instant::now();
            match self.ws_once().await {
                Ok(()) => break,
                Err(e) => {
                    if !self.running.load(Ordering::SeqCst) {
                        break;
                    }
                    if started.elapsed() > Duration::from_secs(30) {
                        attempt = 0;
                    }
                    self.set_ws(false, Some(e.clone()));
                    let delay = [5, 15, 30][attempt.min(2)];
                    self.log("error", format!("行情连接断开：{e}，{delay} 秒后重连"));
                    attempt += 1;
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                }
            }
        }
        self.set_ws(false, None);
    }
}

#[async_trait]
impl ExchangeAdapter for ArcusAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "Arcus"
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
            .fetch_account_state()
            .await
            .map_err(super::AdapterError::Other)?;
        Ok(vec![balance])
    }

    async fn get_positions(&self, _account_id: &str) -> AdapterResult<Vec<Position>> {
        let (snap, _) = self
            .fetch_account_state()
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
        // REST account sync and the market WS run side by side: either one
        // may be down without taking the other with it.
        tokio::join!(self.rest_loop(), self.ws_loop());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markets_rank_by_notional_and_skip_offline() {
        let v = serde_json::json!({
            "markets": [
                {"marketId": 3, "marketDisplayName": "SOL-USD", "status": "ONLINE",
                 "type": "PERPETUAL", "volume24hNotional": "6900000"},
                {"marketId": 1, "marketDisplayName": "BTC-USD", "status": "ONLINE",
                 "type": "PERPETUAL", "volume24hNotional": "32700000"},
                {"marketId": 2, "marketDisplayName": "ETH-USD", "status": "ONLINE",
                 "type": "PERPETUAL", "volume24hNotional": "11300000"},
                {"marketId": 9, "marketDisplayName": "OLD-USD", "status": "OFFLINE",
                 "type": "PERPETUAL", "volume24hNotional": "999000000"}
            ]
        });
        assert_eq!(
            parse_markets(&v, 2).expect("parse"),
            vec!["BTC-USD".to_string(), "ETH-USD".to_string()]
        );
    }

    #[test]
    fn account_parse_derives_pnl_and_positions() {
        let v = serde_json::json!({
            "accountIndex": 0,
            "address": "0x0000000000000000000000000000000000000001",
            "equity": "101000",
            "freeCollateral": "90000",
            "netDeposits": "100000",
            "positions": {
                "1": {"marketDisplayName": "BTC-USD", "side": "LONG", "size": "0.5",
                      "averageEntryPrice": "60000", "markPx": "61000",
                      "unrealizedPnl": "500"},
                "2": {"marketDisplayName": "ETH-USD", "side": "short", "size": "-2",
                      "averageEntryPrice": "3000", "markPx": "2900",
                      "unrealizedPnl": "200"}
            }
        });
        let (equity, available, pnl, positions) = parse_account(&v).expect("parse");
        assert!((equity - 101000.0).abs() < 1e-9);
        assert!((available - 90000.0).abs() < 1e-9);
        assert!((pnl - 1000.0).abs() < 1e-9);
        assert_eq!(positions.len(), 2);
        assert_eq!(positions[0].side, "long");
        assert_eq!(positions[1].side, "short");
        assert!((positions[1].qty - 2.0).abs() < 1e-9);
    }

    #[test]
    fn account_parse_rejects_unexpected_shape() {
        let v = serde_json::json!({"code": 403, "msg": "forbidden"});
        assert!(parse_account(&v).is_err());
    }

    #[test]
    fn side_falls_back_to_size_sign() {
        assert_eq!(parse_side(&Value::Null, 1.5), "long");
        assert_eq!(parse_side(&Value::Null, -1.5), "short");
        assert_eq!(parse_side(&serde_json::json!("weird"), -1.0), "short");
    }

    #[test]
    fn l2_book_frame_parses_snapshot_levels() {
        let contents = serde_json::json!({
            "bids": [["59059.3", "0.203"], ["59058.0", "1.0"]],
            "asks": [["59059.4", "1.347"]],
            "lastSequenceId": 123
        });
        let (bids, asks) = parse_book(&contents);
        assert_eq!(bids.len(), 2);
        assert!((bids[0].0 - 59059.3).abs() < 1e-9);
        assert!((asks[0].1 - 1.347).abs() < 1e-9);
    }

    #[test]
    fn bbo_frame_parses_best_bid_ask() {
        let contents = serde_json::json!({
            "bestBid": {"price": "59059.3", "size": "0.203"},
            "bestAsk": {"price": "59059.4", "size": "1.347"}
        });
        let (bid, ask) = parse_bbo(&contents);
        assert_eq!(bid, Some((59059.3, 0.203)));
        assert_eq!(ask, Some((59059.4, 1.347)));
    }
}
