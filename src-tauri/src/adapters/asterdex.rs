//! AsterDEX read-only adapter.
//!
//! Official endpoints (asterdex/api-docs, V3 recommended; the V1 HMAC
//! scheme below is what the stored API Key / Secret credentials use):
//! - Market data: `wss://fstream.asterdex.com/stream?streams=...`,
//!   `<symbol>@depth5` — every push is a full top-5 snapshot (absolute
//!   quantities), so it is applied with `apply_snapshot`, never as a delta.
//! - Account (read-only, USER_DATA): `GET https://fapi.asterdex.com`
//!   `/fapi/v2/account` + `/fapi/v2/positionRisk`, signed with pure-Rust
//!   HMAC-SHA256 over the query string (`X-MBX-APIKEY` header, `timestamp`
//!   + `recvWindow=5000`, `signature` appended last). No OpenSSL anywhere.
//! - Multi-asset margin: the `total*` fields are USDT-only, so equity /
//!   available / PnL are summed from `assets[]` (marginBalance /
//!   availableBalance / unrealizedProfit) with the totals as fallback;
//!   the displayed settlement currency is the asset with the largest
//!   margin balance (USDT, USDC, USDF, USD1, ...).
//! - No API key/secret (or the Agent-wallet mode, which needs V3 EIP-712
//!   auth — out of scope here): the reason is logged to the console and
//!   the adapter keeps running the public WS book only; REST polling is
//!   skipped and never blocks market data.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use tokio_tungstenite::tungstenite::Message;

use super::book::BookState;
use super::types::{AccountLive, Balance, LivePosition, Order, OrderRequest, Position, Tick};
use super::{AdapterResult, ExchangeAdapter};
use crate::bus::{AppEvent, EventBus};

pub const ASTER_REST_URL: &str = "https://fapi.asterdex.com";
pub const ASTER_WS_URL: &str =
    "wss://fstream.asterdex.com/stream?streams=btcusdt@depth5/ethusdt@depth5";

const REST_TIMEOUT: Duration = Duration::from_secs(10);
const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const PING_INTERVAL: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_secs(10);
const BOOK_DEPTH: usize = 20;

/// Symbols subscribed on the WS (lowercase on the wire) and shown in the
/// Data page (exchange-native uppercase form).
const SYMBOLS: [&str; 2] = ["BTCUSDT", "ETHUSDT"];

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

/// HMAC-SHA256 of `query` with `secret`, lowercase hex. Pure Rust
/// (`hmac` + `sha2`); the secret never leaves this function and never
/// enters a log line or an error message.
fn sign_hmac_sha256(secret: &str, query: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts any key length");
    mac.update(query.as_bytes());
    let out = mac.finalize().into_bytes();
    out.iter().map(|b| format!("{b:02x}")).collect()
}

/// Parse a `/fapi/v2/account` payload (Binance-style v2 shape) into
/// (equity, available, pnl, settlement currency). The `total*` fields are
/// USDT-only, so the per-asset rows win when present.
fn parse_account(v: &Value) -> Result<(f64, f64, f64, String), String> {
    if v.get("totalWalletBalance").is_none() && v.get("assets").is_none() {
        return Err("unexpected /fapi/v2/account shape (no totalWalletBalance, no assets)".to_string());
    }
    let assets = v["assets"].as_array();
    let sum = |field: &str| -> Option<f64> {
        assets.map(|arr| {
            arr.iter()
                .map(|a| f64_from(&a[field]))
                .sum::<f64>()
        })
    };
    let equity = sum("marginBalance")
        .unwrap_or_else(|| f64_from(&v["totalWalletBalance"]) + f64_from(&v["totalUnrealizedProfit"]));
    let available = sum("availableBalance").unwrap_or_else(|| f64_from(&v["availableBalance"]));
    let pnl = sum("unrealizedProfit").unwrap_or_else(|| f64_from(&v["totalUnrealizedProfit"]));
    let currency = assets
        .and_then(|arr| {
            arr.iter()
                .max_by(|a, b| {
                    f64_from(&a["marginBalance"])
                        .partial_cmp(&f64_from(&b["marginBalance"]))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .and_then(|a| a["asset"].as_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "USDT".to_string());
    Ok((equity, available, pnl, currency))
}

/// Parse a `/fapi/v2/positionRisk` payload into live positions.
/// `positionAmt` is signed: its sign is the side; zero rows are skipped.
fn parse_positions(v: &Value) -> Vec<LivePosition> {
    let mut out = Vec::new();
    if let Some(list) = v.as_array() {
        for p in list {
            let amt = f64_from(&p["positionAmt"]);
            if amt == 0.0 {
                continue;
            }
            out.push(LivePosition {
                symbol: p["symbol"].as_str().unwrap_or("").to_string(),
                side: if amt >= 0.0 {
                    "long".to_string()
                } else {
                    "short".to_string()
                },
                qty: amt.abs(),
                entry_price: f64_from(&p["entryPrice"]),
                mark_price: f64_from(&p["markPrice"]),
                unrealized_pnl: f64_from(&p["unRealizedProfit"]),
            });
        }
    }
    out
}

/// Parse one `depthUpdate` frame (`<symbol>@depth5`) into (symbol, bids,
/// asks). Levels are `[price, qty]` string pairs; the frame is a full
/// top-5 snapshot on every push.
fn parse_depth(data: &Value) -> Option<(String, Vec<(f64, f64)>, Vec<(f64, f64)>)> {
    let symbol = data["s"].as_str()?.to_string();
    let side = |v: &Value| -> Vec<(f64, f64)> {
        v.as_array()
            .map(|arr| {
                arr.iter()
                    .map(|lv| (f64_from(&lv[0]), f64_from(&lv[1])))
                    .collect()
            })
            .unwrap_or_default()
    };
    Some((symbol, side(&data["b"]), side(&data["a"])))
}

/// Last REST snapshot, merged with WS status/latency for `AccountLive`.
#[derive(Clone, Default)]
struct Snapshot {
    equity_usd: f64,
    available_usd: f64,
    pnl_usd: f64,
    currency: String,
    positions: Vec<LivePosition>,
}

pub struct AsterdexAdapter {
    id: String,
    account_id: String,
    /// API Key / Secret for the signed USER_DATA reads. None when the
    /// account uses the Agent-wallet mode (V3 EIP-712, unsupported) or
    /// when the stored credentials carry no key pair.
    api_key: Option<String>,
    api_secret: Option<String>,
    http: reqwest::Client,
    bus: EventBus,
    live: Arc<RwLock<HashMap<String, AccountLive>>>,
    running: Arc<AtomicBool>,
    books: Mutex<HashMap<String, BookState>>,
    snapshot: Mutex<Snapshot>,
    ws_up: AtomicBool,
    rest_ok: AtomicBool,
    last_error: Mutex<Option<String>>,
    last_rtt: Mutex<Option<Duration>>,
    last_rest_rtt: Mutex<Option<Duration>>,
}

impl AsterdexAdapter {
    pub fn new(
        id: &str,
        account_id: String,
        api_key: Option<String>,
        api_secret: Option<String>,
        bus: EventBus,
        live: Arc<RwLock<HashMap<String, AccountLive>>>,
    ) -> Self {
        Self {
            id: id.to_string(),
            account_id,
            api_key,
            api_secret,
            http: crate::net::new_http_client(),
            bus,
            live,
            running: Arc::new(AtomicBool::new(false)),
            books: Mutex::new(HashMap::new()),
            snapshot: Mutex::new(Snapshot {
                currency: "USDT".to_string(),
                ..Snapshot::default()
            }),
            ws_up: AtomicBool::new(false),
            rest_ok: AtomicBool::new(false),
            last_error: Mutex::new(None),
            last_rtt: Mutex::new(None),
            last_rest_rtt: Mutex::new(None),
        }
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
            currency: snap.currency,
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

    /// One signed USER_DATA GET. The query is `timestamp` + `recvWindow`
    /// + any extra params, signed last; the secret is never logged.
    async fn signed_get(&self, path: &str, extra: &str) -> Result<Value, String> {
        let (key, secret) = match (&self.api_key, &self.api_secret) {
            (Some(k), Some(s)) => (k.clone(), s.clone()),
            _ => return Err("no API key/secret stored for this account".to_string()),
        };
        let ts = now_ms();
        let query = if extra.is_empty() {
            format!("timestamp={ts}&recvWindow=5000")
        } else {
            format!("{extra}&timestamp={ts}&recvWindow=5000")
        };
        let signature = sign_hmac_sha256(&secret, &query);
        let url = format!("{ASTER_REST_URL}{path}?{query}&signature={signature}");
        let resp = self
            .http
            .get(&url)
            .header("X-MBX-APIKEY", key)
            .send()
            .await
            .map_err(|e| format!("asterdex {path} request failed: {e:?}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let snippet: String = body.chars().take(200).collect();
            return Err(format!("asterdex {path} returned HTTP {status}: {snippet}"));
        }
        resp.json()
            .await
            .map_err(|e| format!("bad {path} response: {e}"))
    }

    /// One REST read of the account + positions. Returns (snapshot, balance).
    async fn fetch_account_state(&self) -> Result<(Snapshot, Balance), String> {
        let v = self.signed_get("/fapi/v2/account", "").await?;
        let (equity, available, pnl, currency) = parse_account(&v)?;
        let pv = self.signed_get("/fapi/v2/positionRisk", "").await?;
        let positions = parse_positions(&pv);
        let snap = Snapshot {
            equity_usd: equity,
            available_usd: available,
            pnl_usd: pnl,
            currency: currency.clone(),
            positions,
        };
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
        match self.fetch_account_state().await {
            Ok((snap, balance)) => {
                *self.snapshot.lock().unwrap() = snap;
                self.bus.publish(AppEvent::Balance(balance));
                self.set_rest(true, Some(started.elapsed()));
            }
            Err(e) => {
                self.log("error", format!("account poll failed: {e}"));
                self.set_rest(false, None);
            }
        }
    }

    /// REST poll loop. With no API key/secret (or the Agent-wallet mode)
    /// the reason is logged once and the loop exits: the WS book keeps
    /// running on its own and public market data is unaffected.
    async fn rest_loop(&self) {
        if self.api_key.is_none() || self.api_secret.is_none() {
            self.log(
                "warn",
                "account read skipped: no API Key/Secret stored (API Key / Secret mode required; \
                 Agent-wallet mode uses V3 EIP-712 auth, unsupported in this phase) — public market data only"
                    .to_string(),
            );
            return;
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

    fn handle_depth(&self, data: &Value) {
        let Some((symbol, bids, asks)) = parse_depth(data) else {
            return;
        };
        let ts = now_ms();
        let mut books = self.books.lock().unwrap();
        let state = books.entry(symbol.clone()).or_insert_with(BookState::new);
        state.apply_snapshot(&bids, &asks);
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

    /// One WS session on the combined depth5 stream. The server sends a
    /// protocol ping every 5 minutes (answered automatically); our own
    /// ping/pong pair measures the RTT.
    async fn ws_once(&self) -> Result<(), String> {
        let (mut ws, _) =
            crate::net::ws::connect_ws(ASTER_WS_URL, WS_CONNECT_TIMEOUT, &self.bus).await?;
        self.set_ws(true, None);
        self.log(
            "info",
            format!("ws connected, subscribed to {}", SYMBOLS.join(", ")),
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
                        return Err("ws send ping failed".to_string());
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
                                    // Combined stream wraps the payload:
                                    // {"stream": "...", "data": {...}}.
                                    let data = v.get("data").unwrap_or(&v);
                                    if data["e"].as_str() == Some("depthUpdate") {
                                        self.handle_depth(data);
                                    }
                                }
                                Err(e) => self.log(
                                    "error",
                                    format!(
                                        "ws frame json parse failed: {e} (first 200 chars: {})",
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
                        Some(Ok(Message::Close(_))) => return Err("asterdex ws closed".to_string()),
                        Some(Err(e)) => return Err(format!("asterdex ws error: {e}")),
                        None => return Err("asterdex ws stream ended".to_string()),
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
                    self.log("error", format!("ws down: {e}"));
                    let delay = [5, 15, 30][attempt.min(2)];
                    attempt += 1;
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                }
            }
        }
        self.set_ws(false, None);
    }
}

#[async_trait]
impl ExchangeAdapter for AsterdexAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        "AsterDEX"
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
        // REST account sync and the market WS run side by side: either one
        // may be down without taking the other with it.
        tokio::join!(self.rest_loop(), self.ws_loop());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_sha256_matches_rfc4231_test_case_2() {
        // RFC 4231, test case 2: key "Jefe", data "what do ya want for
        // nothing?" — the exact scheme AsterDEX V1 signing uses.
        let sig = sign_hmac_sha256("Jefe", "what do ya want for nothing?");
        assert_eq!(
            sig,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn account_parse_sums_per_asset_rows_and_picks_dominant_currency() {
        let v = serde_json::json!({
            "totalWalletBalance": "1000.0",
            "totalUnrealizedProfit": "10.0",
            "availableBalance": "900.0",
            "assets": [
                {"asset": "USDT", "walletBalance": "1000.0", "unrealizedProfit": "10.0",
                 "marginBalance": "1010.0", "availableBalance": "900.0"},
                {"asset": "USDC", "walletBalance": "500.0", "unrealizedProfit": "5.0",
                 "marginBalance": "505.0", "availableBalance": "400.0"}
            ]
        });
        let (equity, available, pnl, currency) = parse_account(&v).expect("parse");
        assert!((equity - 1515.0).abs() < 1e-9);
        assert!((available - 1300.0).abs() < 1e-9);
        assert!((pnl - 15.0).abs() < 1e-9);
        assert_eq!(currency, "USDT");
    }

    #[test]
    fn account_parse_falls_back_to_totals_without_assets() {
        let v = serde_json::json!({
            "totalWalletBalance": "2000.0",
            "totalUnrealizedProfit": "-20.0",
            "availableBalance": "1500.0"
        });
        let (equity, available, pnl, currency) = parse_account(&v).expect("parse");
        assert!((equity - 1980.0).abs() < 1e-9);
        assert!((available - 1500.0).abs() < 1e-9);
        assert!((pnl + 20.0).abs() < 1e-9);
        assert_eq!(currency, "USDT");
    }

    #[test]
    fn account_parse_rejects_unexpected_shape() {
        let v = serde_json::json!({"code": -1121, "msg": "Invalid symbol."});
        assert!(parse_account(&v).is_err());
    }

    #[test]
    fn position_risk_parses_signed_amounts_and_skips_zero_rows() {
        let v = serde_json::json!([
            {"symbol": "BTCUSDT", "positionAmt": "0.5", "entryPrice": "60000.0",
             "markPrice": "61000.0", "unRealizedProfit": "500.0"},
            {"symbol": "ETHUSDT", "positionAmt": "-2.0", "entryPrice": "3000.0",
             "markPrice": "2900.0", "unRealizedProfit": "200.0"},
            {"symbol": "SOLUSDT", "positionAmt": "0.0", "entryPrice": "0",
             "markPrice": "0", "unRealizedProfit": "0"}
        ]);
        let pos = parse_positions(&v);
        assert_eq!(pos.len(), 2);
        assert_eq!(pos[0].symbol, "BTCUSDT");
        assert_eq!(pos[0].side, "long");
        assert!((pos[0].qty - 0.5).abs() < 1e-9);
        assert_eq!(pos[1].side, "short");
        assert!((pos[1].qty - 2.0).abs() < 1e-9);
        assert!((pos[1].unrealized_pnl - 200.0).abs() < 1e-9);
    }

    #[test]
    fn depth5_frame_parses_snapshot_levels() {
        let v = serde_json::json!({
            "stream": "btcusdt@depth5",
            "data": {"e": "depthUpdate", "E": 1571889248277i64, "s": "BTCUSDT",
                     "b": [["59059.3", "0.203"], ["59058.0", "1.0"]],
                     "a": [["59059.4", "1.347"]]}
        });
        let data = v.get("data").unwrap();
        let (symbol, bids, asks) = parse_depth(data).expect("parse");
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(bids.len(), 2);
        assert!((bids[0].0 - 59059.3).abs() < 1e-9);
        assert!((asks[0].1 - 1.347).abs() < 1e-9);
    }
}
