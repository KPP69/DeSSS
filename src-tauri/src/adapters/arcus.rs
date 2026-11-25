//! Arcus read-only adapter.
//!
//! Official endpoints (docs.arcus.xyz/api-reference, updated 2026-11-12):
//! - Market list: `GET https://api.arcus.xyz/v1/markets` — used by the
//!   on-demand catalog (bulk specs + prices + funding).
//! - Market data: `wss://api.arcus.xyz/v1/ws`. The WS is kept open only
//!   as a keepalive (ping every 20s); no book is subscribed — public
//!   market data comes from the on-demand catalog.
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

use super::types::{AccountLive, Balance, LivePosition, Order, OrderRequest, Position};
use super::{f64_from, now_ms, AdapterResult, ExchangeAdapter};
use crate::bus::{AppEvent, EventBus};

pub const ARCUS_REST_URL: &str = "https://api.arcus.xyz";
pub const ARCUS_WS_URL: &str = "wss://api.arcus.xyz/v1/ws";

const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const PING_INTERVAL: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_secs(10);

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
            http: crate::net::shared_http_client(),
            bus,
            live,
            running: Arc::new(AtomicBool::new(false)),
            snapshot: Mutex::new(Snapshot::default()),
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

    /// One WS session, keepalive only: connect, then ping every 20s so a
    /// proxy or router never cuts the idle socket. No book is subscribed —
    /// public market data comes from the on-demand catalog.
    async fn ws_once(&self) -> Result<(), String> {
        let (mut ws, _) =
            crate::net::ws::connect_ws(ARCUS_WS_URL, WS_CONNECT_TIMEOUT, &self.bus).await?;
        self.set_ws(true, None);
        self.log("info", "行情已连接（仅保活，不订阅盘口）".to_string());

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
    /// A dead WS only flips ws_up off — the account keeps its REST-driven
    /// live state (see live_status). A session that lived longer than 30s
    /// resets the ladder, so a healthy link that drops once retries fast.
    /// A TLS close without close_notify (unexpected-eof) is a routine
    /// drop: warn and reconnect, never an error red dot.
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
                    let delay = [5, 15, 30][attempt.min(2)];
                    if super::is_tls_unexpected_eof(&e) {
                        *self.last_error.lock().unwrap() = None;
                        self.set_ws(false, None);
                        self.log(
                            "warn",
                            format!("行情连接断开（TLS 对端关闭），{delay} 秒后重连"),
                        );
                    } else {
                        self.set_ws(false, Some(e.clone()));
                        self.log("error", format!("行情连接断开：{e}，{delay} 秒后重连"));
                    }
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
}
