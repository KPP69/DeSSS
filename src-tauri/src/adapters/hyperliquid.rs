//! Hyperliquid read-only adapter, shared by Hyperliquid mainnet and
//! Entropy (HIP-3, `dex = "io"`).
//!
//! - Market data: none by default. The WS is kept open only as a
//!   keepalive (ping every 20s) so the account status stays live; no book
//!   is subscribed — public market data comes from the on-demand catalog.
//! - Account (read-only): `POST https://api.hyperliquid.xyz/info`
//!   `clearinghouseState` — real equity, withdrawable margin, open
//!   positions and unrealised PnL. No order placement in this phase.
//! - Unified account: one master wallet shares a single pool across
//!   mainnet perp (`dex = ""`), Entropy (`dex = "io"`) and spot. The
//!   account read always aggregates all three, so the Hyperliquid and
//!   Entropy adapters publish identical numbers for the same wallet.

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

pub const HL_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
pub const HL_INFO_URL: &str = "https://api.hyperliquid.xyz/info";

const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const PING_INTERVAL: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// One spot balance row from `spotClearinghouseState`.
#[derive(Clone, Debug)]
struct SpotBalance {
    coin: String,
    total: f64,
    hold: f64,
    entry_ntl: f64,
}

fn parse_spot_balances(v: &Value) -> Vec<SpotBalance> {
    v["balances"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|b| {
                    let coin = b["coin"].as_str()?.to_string();
                    Some(SpotBalance {
                        coin,
                        total: f64_from(&b["total"]),
                        hold: f64_from(&b["hold"]),
                        entry_ntl: f64_from(&b["entryNtl"]),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Merge spot balances into a perp snapshot (mainnet only). Under a unified
/// account the perp `clearinghouseState` alone shows $0: the USDC pool and
/// spot holdings live in `spotClearinghouseState`, which the docs call the
/// source of truth for the trading-account balance under unified/portfolio
/// margin. Total equity = perp account value + spot USDC + spot holdings
/// valued at the live book mid (or entry notional when no book is known);
/// available = perp withdrawable + free spot USDC.
fn merge_spot(
    perp: Snapshot,
    spot: &[SpotBalance],
    mark: &dyn Fn(&str) -> Option<f64>,
) -> Snapshot {
    let mut snap = perp;
    let mut spot_pnl = 0.0;
    for b in spot {
        if b.coin == "USDC" {
            snap.equity_usd += b.total;
            snap.available_usd += (b.total - b.hold).max(0.0);
            continue;
        }
        if b.total == 0.0 {
            continue;
        }
        let entry = if b.total != 0.0 {
            b.entry_ntl / b.total
        } else {
            0.0
        };
        let mid = mark(&b.coin).unwrap_or(entry);
        let value = mid * b.total;
        let upnl = value - b.entry_ntl;
        spot_pnl += upnl;
        snap.equity_usd += value;
        snap.positions.push(LivePosition {
            symbol: format!("{} (spot)", b.coin),
            side: "long".to_string(),
            qty: b.total,
            entry_price: entry,
            mark_price: mid,
            unrealized_pnl: upnl,
        });
    }
    snap.pnl_usd += spot_pnl;
    snap
}

/// Add one perp snapshot into another (same master wallet, different
/// dex): equity, available margin and PnL sum, positions concatenate.
fn merge_perp(mut dst: Snapshot, src: Snapshot) -> Snapshot {
    dst.equity_usd += src.equity_usd;
    dst.available_usd += src.available_usd;
    dst.pnl_usd += src.pnl_usd;
    dst.positions.extend(src.positions);
    dst
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
    snapshot: Mutex<Snapshot>,
    /// WS health and REST health are tracked separately so a dead market
    /// WS never hides a healthy REST account read (see live_status).
    ws_up: AtomicBool,
    rest_ok: AtomicBool,
    last_error: Mutex<Option<String>>,
    last_rtt: Mutex<Option<Duration>>,
    last_rest_rtt: Mutex<Option<Duration>>,
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
        Self {
            id: id.to_string(),
            dex: dex.map(|d| d.to_string()),
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
    /// while the WS is up, else the last REST request time. Frame
    /// staleness and wall-clock subtraction are never used — a stale frame
    /// timestamp is what made latency run away to 1.5M ms.
    fn latency_ms(&self) -> Option<u64> {
        if self.ws_up.load(Ordering::SeqCst) {
            return self.last_rtt.lock().unwrap().map(|r| r.as_millis() as u64);
        }
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

    /// WS state changed: record it (and the error, if any) and republish.
    fn set_ws(&self, up: bool, error: Option<String>) {
        self.ws_up.store(up, Ordering::SeqCst);
        if up {
            *self.last_error.lock().unwrap() = None;
        } else if let Some(e) = error {
            *self.last_error.lock().unwrap() = Some(e);
        }
        self.publish_live();
    }

    /// REST state changed: record it and republish.
    fn set_rest(&self, ok: bool, rtt: Option<Duration>) {
        self.rest_ok.store(ok, Ordering::SeqCst);
        if let Some(r) = rtt {
            *self.last_rest_rtt.lock().unwrap() = Some(r);
        }
        self.publish_live();
    }

    /// One REST read of clearinghouseState for one dex (`None` = mainnet
    /// perp, `Some("io")` = Entropy). Returns (snapshot, balance).
    async fn fetch_clearinghouse(&self, dex: Option<&str>) -> Result<(Snapshot, Balance), String> {
        let mut body = serde_json::json!({
            "type": "clearinghouseState",
            "user": self.address,
        });
        if let Some(dex) = dex {
            body["dex"] = Value::String(dex.to_string());
        }
        let resp = self
            .http
            .post(HL_INFO_URL)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Hyperliquid 账户请求失败：{e:?}"))?;
        if !resp.status().is_success() {
            return Err(format!("Hyperliquid 账户接口返回 HTTP {}", resp.status()));
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| format!("账户响应解析失败：{e}"))?;
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
                    side: if szi >= 0.0 {
                        "long".to_string()
                    } else {
                        "short".to_string()
                    },
                    qty: szi.abs(),
                    entry_price: f64_from(&p["entryPx"]),
                    mark_price: f64_from(&p["positionValue"]) / szi.abs().max(1e-12),
                    unrealized_pnl: upnl,
                });
            }
        }
        let snap = Snapshot {
            equity_usd: equity,
            available_usd: withdrawable,
            pnl_usd: pnl,
            positions,
        };
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

    /// One REST read of spotClearinghouseState (mainnet only; the dex
    /// variant has no spot side). Returns the raw balance rows.
    async fn fetch_spot_state(&self) -> Result<Vec<SpotBalance>, String> {
        let body = serde_json::json!({ "type": "spotClearinghouseState", "user": self.address });
        let resp = self
            .http
            .post(HL_INFO_URL)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Hyperliquid 现货账户请求失败：{e:?}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "hyperliquid spot info returned HTTP {}",
                resp.status()
            ));
        }
        let v: Value = resp
            .json()
            .await
            .map_err(|e| format!("现货账户响应解析失败：{e}"))?;
        Ok(parse_spot_balances(&v))
    }

    /// Full account read: mainnet perp + Entropy (dex "io") + spot, all
    /// aggregated into one unified snapshot for this master wallet. The
    /// mainnet leg is required; the io and spot legs only warn on failure
    /// (an unused io sub-account or a missing spot pool must not hide the
    /// main account). Because both the Hyperliquid and the Entropy
    /// adapters run this same aggregation, the two sides always show the
    /// same available balance and positions.
    async fn fetch_account_state(&self) -> Result<(Snapshot, Balance), String> {
        let (main, io, spot) = tokio::join!(
            self.fetch_clearinghouse(None),
            self.fetch_clearinghouse(Some("io")),
            self.fetch_spot_state(),
        );
        let (mut snap, _) = main?;
        match io {
            Ok((io_snap, _)) => snap = merge_perp(snap, io_snap),
            Err(e) => {
                self.log("warn", format!("Entropy 账户读取失败，只用主站：{e}"));
            }
        }
        match spot {
            Ok(spot) => {
                // No book is subscribed any more, so spot holdings are
                // valued at their entry notional (unrealised PnL 0) until
                // a live mark exists.
                snap = merge_spot(snap, &spot, &|_| None);
            }
            Err(e) => {
                self.log("warn", format!("现货账户读取失败，只用永续仓位：{e}"));
            }
        }
        let balance = Balance {
            adapter_id: self.id.clone(),
            account_id: self.account_id.clone(),
            asset: "USDC".to_string(),
            free: snap.available_usd,
            locked: (snap.equity_usd - snap.available_usd).max(0.0),
            total: snap.equity_usd,
            equity_usd: snap.equity_usd,
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

    /// REST poll loop, independent of the market WS: as long as REST works,
    /// equity and positions keep showing even while the WS is down. The
    /// first poll also fills the snapshot that dex symbol resolution
    /// needs (current holdings' coins).
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
    /// public market data comes from the on-demand catalog. REST polling
    /// lives in rest_loop() and is unaffected by WS failures.
    async fn ws_once(&self) -> Result<(), String> {
        let (mut ws, _) =
            crate::net::ws::connect_ws(HL_WS_URL, WS_CONNECT_TIMEOUT, &self.bus).await?;
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
                    let ping = serde_json::json!({"method": "ping"});
                    if ws.send(Message::Text(ping.to_string().into())).await.is_err() {
                        return Err("行情心跳发送失败".to_string());
                    }
                    ping_sent = Some(Instant::now());
                }
                _ = stop_i.tick() => {}
                msg = ws.next() => {
                    match msg {
                        Some(Ok(Message::Text(t))) => {
                            let text: &str = t.as_ref();
                            if let Ok(v) = serde_json::from_str::<Value>(text) {
                                if v["channel"].as_str() == Some("pong") {
                                    if let Some(sent) = ping_sent.take() {
                                        *self.last_rtt.lock().unwrap() = Some(sent.elapsed());
                                        self.publish_live();
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Close(_))) => return Err("Hyperliquid 行情连接已关闭".to_string()),
                        Some(Err(e)) => return Err(format!("Hyperliquid 行情出错：{e}")),
                        None => return Err("Hyperliquid 行情流已结束".to_string()),
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
impl ExchangeAdapter for HyperliquidAdapter {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        if self.dex.is_some() {
            "Entropy"
        } else {
            "Hyperliquid"
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
    fn merge_perp_adds_two_dex_snapshots() {
        let main = Snapshot {
            equity_usd: 1000.0,
            available_usd: 800.0,
            pnl_usd: 10.0,
            positions: vec![LivePosition {
                symbol: "BTC".to_string(),
                side: "long".to_string(),
                qty: 0.1,
                entry_price: 58000.0,
                mark_price: 58100.0,
                unrealized_pnl: 10.0,
            }],
        };
        let io = Snapshot {
            equity_usd: 200.0,
            available_usd: 150.0,
            pnl_usd: -5.0,
            positions: vec![LivePosition {
                symbol: "io:NVDA".to_string(),
                side: "short".to_string(),
                qty: 2.0,
                entry_price: 143.0,
                mark_price: 140.0,
                unrealized_pnl: 6.0,
            }],
        };
        let merged = merge_perp(main, io);
        assert!((merged.equity_usd - 1200.0).abs() < 1e-9);
        assert!((merged.available_usd - 950.0).abs() < 1e-9);
        assert!((merged.pnl_usd - 5.0).abs() < 1e-9);
        assert_eq!(merged.positions.len(), 2);
    }

    #[test]
    fn spot_merge_adds_usdc_and_valued_holdings_to_unified_equity() {
        let perp = Snapshot {
            equity_usd: 0.0,
            available_usd: 0.0,
            pnl_usd: 0.0,
            positions: vec![],
        };
        let spot = vec![
            SpotBalance {
                coin: "USDC".to_string(),
                total: 100.0,
                hold: 5.0,
                entry_ntl: 0.0,
            },
            SpotBalance {
                coin: "BTC".to_string(),
                total: 0.1,
                hold: 0.0,
                entry_ntl: 6000.0,
            },
        ];
        let mark = |coin: &str| if coin == "BTC" { Some(65000.0) } else { None };
        let merged = merge_spot(perp, &spot, &mark);
        assert!((merged.equity_usd - 6600.0).abs() < 1e-6);
        assert!((merged.available_usd - 95.0).abs() < 1e-6);
        assert!((merged.pnl_usd - 500.0).abs() < 1e-6);
        assert_eq!(merged.positions.len(), 1);
        assert_eq!(merged.positions[0].symbol, "BTC (spot)");
        assert!((merged.positions[0].entry_price - 60000.0).abs() < 1e-6);
        assert!((merged.positions[0].mark_price - 65000.0).abs() < 1e-6);
    }

    #[test]
    fn spot_merge_without_mark_falls_back_to_entry_notional() {
        let perp = Snapshot::default();
        let spot = vec![SpotBalance {
            coin: "SOL".to_string(),
            total: 2.0,
            hold: 0.0,
            entry_ntl: 300.0,
        }];
        let merged = merge_spot(perp, &spot, &|_| None);
        assert!((merged.equity_usd - 300.0).abs() < 1e-6);
        assert!((merged.pnl_usd - 0.0).abs() < 1e-6);
    }
}
