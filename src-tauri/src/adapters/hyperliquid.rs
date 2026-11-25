//! Hyperliquid adapter, shared by Hyperliquid mainnet and
//! Entropy (HIP-3, `dex = "io"`).
//!
//! - Market data: none. The adapter opens no WS connection at all —
//!   public market data comes from the on-demand catalog.
//! - Account (read-only): `POST https://api.hyperliquid.xyz/info`
//!   `clearinghouseState` — real equity, withdrawable margin, open
//!   positions and unrealised PnL.
//! - Orders (phase 5A §4): real limit / post-only / IOC orders via
//!   `POST /exchange` with pure-Rust EIP-712 L1 signing
//!   (`adapters/hl_signing.rs`, golden-vector tested against the
//!   official Python SDK). The signing key is the account's
//!   Agent/API wallet private key, decoded at connect time; accounts
//!   without one stay read-only. Orders are one-shot sends — a
//!   write is NEVER retried, and a send with no usable answer
//!   returns `UnknownOutcome`, never a guessed fill.
//! - Unified account: one master wallet shares a single pool across
//!   mainnet perp (`dex = ""`), Entropy (`dex = "io"`) and spot. The
//!   account read always aggregates all three, so the Hyperliquid and
//!   Entropy adapters publish identical numbers for the same wallet.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;

use super::hl_signing::{self, Mp, NonceAllocator};
use super::reconcile::ReconcileState;
use super::types::{
    AccountLive, Balance, LivePosition, Order, OrderInfo, OrderRequest, OrderStatus, Position,
    TradeInfo,
};
use super::{f64_from, now_ms, AdapterError, AdapterResult, ExchangeAdapter};
use crate::bus::{AppEvent, EventBus};

pub const HL_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
pub const HL_EXCHANGE_URL: &str = "https://api.hyperliquid.xyz/exchange";

/// Asset-index cache TTL: the meta universe changes only when the
/// exchange lists/delists a market.
const ASSET_META_TTL: Duration = Duration::from_secs(300);

/// One-shot write timeout: an order send that hangs longer than this
/// is an unknown outcome, not a failure — the caller reconciles.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Last REST snapshot, published as `AccountLive`.
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
    rest_ok: AtomicBool,
    last_error: Mutex<Option<String>>,
    last_rest_rtt: Mutex<Option<Duration>>,
    /// Post-fill reconciliation grace (spec §3): polls skip their write
    /// while the account is inside the window.
    reconcile: Arc<ReconcileState>,
    /// Woken by `wake_sync` for an immediate out-of-cycle poll.
    wake: Arc<tokio::sync::Notify>,
    /// Agent/API wallet private key for order signing, when the
    /// account saved one. Never logged, never returned, never sent
    /// anywhere except into the EIP-712 signature. `None` means the
    /// account is read-only: reads work, orders fail with a clear
    /// message.
    agent_secret: Option<[u8; 32]>,
    /// Address the agent key signs as (derived from the key).
    agent_address: Option<String>,
    /// Cached asset-index universe for order placement (meta +
    /// perpDexs offset for HIP-3). Refreshed lazily, 5 min TTL.
    asset_meta: Mutex<Option<AssetMeta>>,
}

/// Cached `meta` universe for one dex: symbol order IS the asset
/// index (main dex offset 0; HIP-3 dexes start at 110000, +10000 per
/// further dex — the SDK's `perp_dex_to_offset` rule).
#[derive(Clone)]
struct AssetMeta {
    fetched_at: Instant,
    universe: Vec<String>,
    dex_offset: u32,
}

impl HyperliquidAdapter {
    pub fn new(
        id: &str,
        dex: Option<&str>,
        address: String,
        account_id: String,
        bus: EventBus,
        live: Arc<RwLock<HashMap<String, AccountLive>>>,
        reconcile: Arc<ReconcileState>,
        agent_secret: Option<[u8; 32]>,
    ) -> Self {
        let agent_address = agent_secret
            .as_ref()
            .map(|s| super::asterdex::address_from_secret(s));
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
            rest_ok: AtomicBool::new(false),
            last_error: Mutex::new(None),
            last_rest_rtt: Mutex::new(None),
            reconcile,
            wake: Arc::new(tokio::sync::Notify::new()),
            agent_secret,
            agent_address,
            asset_meta: Mutex::new(None),
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
        let resp = crate::net::send_read(|| self.http.post(HL_INFO_URL).json(&body))
            .await
            .map_err(|e| format!("Hyperliquid 账户请求失败：{e}"))?;
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
        let resp = crate::net::send_read(|| self.http.post(HL_INFO_URL).json(&body))
            .await
            .map_err(|e| format!("Hyperliquid 现货账户请求失败：{e}"))?;
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
        // Post-fill grace (spec §3): a poll landing inside the window
        // would return a pre-fill snapshot and overwrite the fresh
        // local estimate — skip the round entirely.
        if self.reconcile.in_grace(&self.account_id) {
            return;
        }
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

    /// REST poll loop: as long as REST works, equity and positions keep
    /// showing. The first poll also fills the snapshot that dex symbol
    /// resolution needs (current holdings' coins).
    async fn rest_loop(&self) {
        self.poll_account().await;
        let mut tick = tokio::time::interval(POLL_INTERVAL);
        tick.tick().await;
        while self.running.load(Ordering::SeqCst) {
            // Either the 10s tick or an out-of-cycle wake (position
            // reconciliation) triggers the next poll.
            tokio::select! {
                _ = tick.tick() => {}
                _ = self.wake.notified() => {}
            }
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            self.poll_account().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Order placement (phase 5A §4)
// ---------------------------------------------------------------------------

/// A wire price/size string: Decimal, at most 8 decimals (the SDK's
/// `float_to_wire` rule), trailing zeros stripped.
fn decimal_to_wire(v: f64) -> Result<String, String> {
    // `to_decimal` maps NaN / infinity to zero; the caller checks
    // > 0, so an invalid number can never reach the wire.
    let d = crate::risk::precision::to_decimal(v);
    if d.scale() > 8 {
        return Err("价格/数量精度超过 8 位小数".to_string());
    }
    Ok(d.normalize().to_string())
}

/// `openOrders` / `frontendOpenOrders` side: "A" is the ask (sell),
/// "B" is the bid (buy).
fn hl_side_to_side(s: &str) -> String {
    if s == "A" {
        "sell".to_string()
    } else {
        "buy".to_string()
    }
}

/// HIP-3 coins come back prefixed with the dex name (`io:BTC`);
/// our symbols are bare, so the prefix is stripped.
fn strip_dex_prefix(coin: &str) -> String {
    coin.split_once(':')
        .map(|(_, rest)| rest)
        .unwrap_or(coin)
        .to_string()
}

/// Map an `orderStatus` status string onto the canonical enum.
/// Anything unrecognised stays Unresolved — a status we do not
/// understand is never guessed into Filled or Canceled.
fn map_order_status(s: &str) -> OrderStatus {
    match s {
        "open" | "triggered" => OrderStatus::Open,
        "filled" => OrderStatus::Filled,
        s if s == "canceled" || s.ends_with("Canceled") => OrderStatus::Canceled,
        s if s == "rejected" || s.ends_with("Rejected") => OrderStatus::Rejected,
        _ => OrderStatus::Unresolved,
    }
}

/// Parse one `orderStatus` response into an OrderInfo. `fills_avg`
/// is the volume-weighted average fill price from the fills query
/// (None when the order never filled or the fills query failed — the
/// caller falls back to the limit price and logs it).
fn parse_order_status_response(
    v: &Value,
    exchange_id: &str,
    account_id: &str,
    cloid: &str,
    fills_avg: Option<f64>,
) -> Result<OrderInfo, String> {
    if v["status"].as_str() == Some("unknownOid") {
        return Err("unknownOid".to_string());
    }
    let inner = &v["order"];
    let o = &inner["order"];
    if o.is_null() {
        return Err("订单状态响应无法解析".to_string());
    }
    let status = map_order_status(inner["status"].as_str().unwrap_or(""));
    let orig_sz = f64_from(&o["origSz"]);
    let remaining = f64_from(&o["sz"]);
    let filled = (orig_sz - remaining).max(0.0);
    let status = if status == OrderStatus::Open && filled > 0.0 {
        OrderStatus::PartiallyFilled
    } else {
        status
    };
    let limit_px = f64_from(&o["limitPx"]);
    let avg = if filled > 0.0 {
        fills_avg.or(Some(limit_px))
    } else {
        None
    };
    let tif = o["tif"].as_str().unwrap_or("Gtc");
    let order_type = match tif {
        "Alo" => "post_only",
        "Ioc" => "ioc",
        _ => "limit",
    };
    Ok(OrderInfo {
        order_id: o["oid"].as_u64().map(|n| n.to_string()).unwrap_or_default(),
        client_order_id: o["cloid"]
            .as_str()
            .map(|c| c.to_string())
            .filter(|c| !c.is_empty())
            .unwrap_or_else(|| cloid.to_string()),
        exchange_id: exchange_id.to_string(),
        account_id: account_id.to_string(),
        symbol: strip_dex_prefix(o["coin"].as_str().unwrap_or("")),
        side: hl_side_to_side(o["side"].as_str().unwrap_or("B")),
        order_type: order_type.to_string(),
        status,
        price: Some(limit_px),
        quantity: orig_sz,
        filled_quantity: filled,
        remaining_quantity: remaining,
        average_fill_price: avg,
        error: None,
        created_at: o["timestamp"].as_i64().unwrap_or(0),
        updated_at: inner["statusTimestamp"].as_i64().unwrap_or(0),
    })
}

/// Parse an `openOrders` response into wire orders.
fn parse_open_orders(v: &Value, exchange_id: &str, account_id: &str) -> Result<Vec<Order>, String> {
    let arr = v
        .as_array()
        .ok_or_else(|| "挂单列表响应无法解析".to_string())?;
    Ok(arr
        .iter()
        .filter_map(|o| {
            let oid = o["oid"].as_u64()?;
            Some(Order {
                order_id: oid.to_string(),
                client_order_id: String::new(),
                exchange_id: exchange_id.to_string(),
                account_id: account_id.to_string(),
                symbol: strip_dex_prefix(o["coin"].as_str().unwrap_or("")),
                side: hl_side_to_side(o["side"].as_str().unwrap_or("B")),
                order_type: "limit".to_string(),
                quantity: f64_from(&o["sz"]),
                price: Some(f64_from(&o["limitPx"])),
                status: OrderStatus::Open,
                ts: o["timestamp"].as_i64().unwrap_or(0),
            })
        })
        .collect())
}

/// Parse a `userFillsByTime` response into TradeInfo rows for one
/// oid. The exchange returns the fills newest-first; order here is
/// kept as returned (the store sorts by timestamp on display).
fn parse_fills(
    v: &Value,
    exchange_id: &str,
    account_id: &str,
    cloid: &str,
    oid: u64,
) -> Vec<TradeInfo> {
    let Some(arr) = v.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .filter(|f| f["oid"].as_u64() == Some(oid))
        .map(|f| TradeInfo {
            trade_id: f["tid"].as_u64().map(|n| n.to_string()).unwrap_or_default(),
            exchange_id: exchange_id.to_string(),
            order_id: oid.to_string(),
            client_order_id: cloid.to_string(),
            account_id: account_id.to_string(),
            symbol: strip_dex_prefix(f["coin"].as_str().unwrap_or("")),
            side: hl_side_to_side(f["side"].as_str().unwrap_or("B")),
            price: f64_from(&f["px"]),
            quantity: f64_from(&f["sz"]),
            fee: Some(f64_from(&f["fee"])),
            fee_currency: Some(f["feeToken"].as_str().unwrap_or("USDC").trim().to_string()),
            timestamp: f["time"].as_i64().unwrap_or(0),
        })
        .collect()
}

impl HyperliquidAdapter {
    /// The dex this adapter trades on: `""` for mainnet perp,
    /// `"io"` for Entropy.
    fn dex_name(&self) -> &str {
        self.dex.as_deref().unwrap_or("")
    }

    /// One `/info` read (read-only: retry-once via `send_read`).
    async fn post_info(&self, body: Value) -> Result<Value, AdapterError> {
        let resp = crate::net::send_read(|| self.http.post(HL_INFO_URL).json(&body))
            .await
            .map_err(|e| AdapterError::Other(format!("Hyperliquid 查询失败：{e}")))?;
        if !resp.status().is_success() {
            return Err(AdapterError::Other(format!(
                "Hyperliquid 查询返回 HTTP {}",
                resp.status()
            )));
        }
        resp.json()
            .await
            .map_err(|e| AdapterError::Other(format!("Hyperliquid 响应解析失败：{e}")))
    }

    /// Resolve a symbol to its asset index, using the cached meta
    /// universe (5 min TTL). Main dex: index in `meta.universe`;
    /// Entropy: 110000 + index in the `io` universe (+10000 per
    /// further HIP-3 dex, per the SDK's perpDexs offset rule).
    async fn resolve_asset(&self, symbol: &str) -> Result<u32, AdapterError> {
        {
            let cache = self.asset_meta.lock().unwrap();
            if let Some(m) = cache.as_ref() {
                if m.fetched_at.elapsed() < ASSET_META_TTL {
                    return m
                        .universe
                        .iter()
                        .position(|n| n == symbol)
                        .map(|i| m.dex_offset + i as u32)
                        .ok_or_else(|| {
                            AdapterError::Other(format!("{symbol} 不在该交易所的交易品种中"))
                        });
                }
            }
        }
        let dex = self.dex_name();
        let mut meta_body = serde_json::json!({"type": "meta"});
        if !dex.is_empty() {
            meta_body["dex"] = Value::String(dex.to_string());
        }
        let meta = self.post_info(meta_body).await?;
        let universe: Vec<String> = meta["universe"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|u| u["name"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        if universe.is_empty() {
            return Err(AdapterError::Other(
                "Hyperliquid 品种列表为空，无法解析交易对".to_string(),
            ));
        }
        let mut dex_offset = 0u32;
        if !dex.is_empty() {
            let dexs = self
                .post_info(serde_json::json!({"type": "perpDexs"}))
                .await?;
            let names: Vec<String> = dexs
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|d| d["name"].as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let pos = names.iter().position(|n| n == dex).ok_or_else(|| {
                AdapterError::Other(format!("交易所未返回 {dex} dex，无法解析交易对"))
            })?;
            // perp_dexs()[0] is mainnet (""); builder dexes start at
            // 110000, +10000 each.
            dex_offset = 110_000 + (pos.saturating_sub(1) as u32) * 10_000;
        }
        let asset = universe
            .iter()
            .position(|n| n == symbol)
            .map(|i| dex_offset + i as u32)
            .ok_or_else(|| AdapterError::Other(format!("{symbol} 不在该交易所的交易品种中")))?;
        *self.asset_meta.lock().unwrap() = Some(AssetMeta {
            fetched_at: Instant::now(),
            universe,
            dex_offset,
        });
        Ok(asset)
    }

    /// The agent key, or a clear error when the account never saved
    /// one (read-only account).
    fn signing_key(&self) -> Result<([u8; 32], String), AdapterError> {
        match (&self.agent_secret, &self.agent_address) {
            (Some(secret), Some(addr)) => Ok((*secret, addr.clone())),
            _ => Err(AdapterError::Other(
                "该账户未保存 Agent 钱包私钥，无法下单（请在账户中心重新保存凭证）".to_string(),
            )),
        }
    }

    /// One `/exchange` write. One-shot, no retry, 30s timeout.
    /// A transport failure, a non-2xx status, or an unreadable body
    /// is `UnknownOutcome` — the order may have landed, so the
    /// caller must reconcile by cloid.
    async fn post_exchange(
        &self,
        action_json: Value,
        action_mp: &Mp<'_>,
        nonce: u64,
    ) -> Result<Value, AdapterError> {
        let (secret, _) = self.signing_key()?;
        let sig =
            hl_signing::sign_l1_action(&secret, action_mp, nonce).map_err(AdapterError::Other)?;
        let body = serde_json::json!({
            "action": action_json,
            "nonce": nonce,
            "signature": {"r": sig.r, "s": sig.s, "v": sig.v},
            "vaultAddress": null,
            "expiresAfter": null,
        });
        let resp = self
            .http
            .post(HL_EXCHANGE_URL)
            .json(&body)
            .timeout(WRITE_TIMEOUT)
            .send()
            .await
            .map_err(|e| AdapterError::UnknownOutcome(format!("下单请求未收到交易所响应：{e}")))?;
        if !resp.status().is_success() {
            return Err(AdapterError::UnknownOutcome(format!(
                "交易所返回 HTTP {}，订单状态未知",
                resp.status()
            )));
        }
        resp.json().await.map_err(|e| {
            AdapterError::UnknownOutcome(format!("交易所响应无法解析，订单状态未知：{e}"))
        })
    }

    /// Volume-weighted average fill price of one order, from
    /// `userFillsByTime` (matched by oid). `None` when there are no
    /// fills yet or the query fails — the caller falls back to the
    /// limit price and logs it.
    async fn fills_avg(&self, oid: u64, created_at_ms: i64) -> Option<f64> {
        let v = self
            .post_info(serde_json::json!({
                "type": "userFillsByTime",
                "user": self.address,
                "startTime": created_at_ms,
                "aggregateByTime": true,
            }))
            .await
            .ok()?;
        let arr = v.as_array()?;
        let mut qty = 0.0f64;
        let mut notional = 0.0f64;
        for f in arr {
            if f["oid"].as_u64() != Some(oid) {
                continue;
            }
            let sz = f64_from(&f["sz"]);
            let px = f64_from(&f["px"]);
            qty += sz;
            notional += sz * px;
        }
        if qty > 0.0 {
            Some(notional / qty)
        } else {
            None
        }
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
        let mut body = serde_json::json!({
            "type": "openOrders",
            "user": self.address,
        });
        let dex = self.dex_name();
        if !dex.is_empty() {
            body["dex"] = Value::String(dex.to_string());
        }
        let v = self.post_info(body).await?;
        parse_open_orders(&v, &self.id, &self.account_id).map_err(AdapterError::Other)
    }

    async fn place_order(&self, req: OrderRequest) -> AdapterResult<Order> {
        let (_secret, agent_addr) = self.signing_key()?;
        let asset = self.resolve_asset(&req.symbol).await?;
        let price = req
            .price
            .filter(|p| *p > 0.0)
            .ok_or_else(|| AdapterError::Other("限价单必须有大于 0 的价格".to_string()))?;
        if req.quantity <= 0.0 {
            return Err(AdapterError::Other("下单数量必须大于 0".to_string()));
        }
        let tif = match req.order_type.as_str() {
            "limit" => "Gtc",
            "post_only" => "Alo",
            "ioc" => "Ioc",
            other => {
                return Err(AdapterError::Other(format!(
                    "不支持的订单类型：{other}（仅支持 limit / post_only / ioc）"
                )))
            }
        };
        let price_wire = decimal_to_wire(price).map_err(AdapterError::Other)?;
        let qty_wire = decimal_to_wire(req.quantity).map_err(AdapterError::Other)?;
        let cloid = req
            .client_order_id
            .clone()
            .filter(|c| !c.is_empty())
            .unwrap_or_else(hl_signing::generate_cloid);
        let nonce = NonceAllocator::next(&agent_addr);
        let action_mp = Mp::Map(vec![
            ("type", Mp::Str("order")),
            (
                "orders",
                Mp::Array(vec![Mp::Map(vec![
                    ("a", Mp::UInt(asset as u64)),
                    ("b", Mp::Bool(req.side == "buy")),
                    ("p", Mp::Str(&price_wire)),
                    ("s", Mp::Str(&qty_wire)),
                    ("r", Mp::Bool(false)),
                    (
                        "t",
                        Mp::Map(vec![("limit", Mp::Map(vec![("tif", Mp::Str(tif))]))]),
                    ),
                    ("c", Mp::Str(&cloid)),
                ])]),
            ),
            ("grouping", Mp::Str("na")),
        ]);
        let action_json = serde_json::json!({
            "type": "order",
            "orders": [{
                "a": asset,
                "b": req.side == "buy",
                "p": price_wire,
                "s": qty_wire,
                "r": false,
                "t": {"limit": {"tif": tif}},
                "c": cloid,
            }],
            "grouping": "na",
        });
        let v = self.post_exchange(action_json, &action_mp, nonce).await?;
        if v["status"].as_str() == Some("err") {
            return Err(AdapterError::Other(format!(
                "交易所拒绝下单：{}",
                v["response"].as_str().unwrap_or("未知原因")
            )));
        }
        let statuses = v["response"]["data"]["statuses"]
            .as_array()
            .ok_or_else(|| {
                AdapterError::UnknownOutcome("下单响应中没有订单状态，订单状态未知".to_string())
            })?;
        let first = statuses.first().ok_or_else(|| {
            AdapterError::UnknownOutcome("下单响应中没有订单状态，订单状态未知".to_string())
        })?;
        let (oid, status) = if let Some(resting) = first.get("resting") {
            (
                resting["oid"].as_u64().unwrap_or(0).to_string(),
                OrderStatus::Open,
            )
        } else if let Some(filled) = first.get("filled") {
            (
                filled["oid"].as_u64().unwrap_or(0).to_string(),
                OrderStatus::Filled,
            )
        } else if let Some(err) = first.get("error").and_then(|e| e.as_str()) {
            return Err(AdapterError::Other(format!("交易所拒绝下单：{err}")));
        } else {
            return Err(AdapterError::UnknownOutcome(
                "下单响应状态无法识别，订单状态未知".to_string(),
            ));
        };
        Ok(Order {
            order_id: oid,
            client_order_id: cloid,
            exchange_id: self.id.clone(),
            account_id: req.account_id,
            symbol: req.symbol,
            side: req.side,
            order_type: req.order_type,
            quantity: req.quantity,
            price: Some(price),
            status,
            ts: now_ms(),
        })
    }

    async fn cancel_order(
        &self,
        _account_id: &str,
        order_id: &str,
        symbol: &str,
    ) -> AdapterResult<()> {
        let (_secret, agent_addr) = self.signing_key()?;
        let asset = self.resolve_asset(symbol).await?;
        let (action_mp, action_json) = if order_id.starts_with("0x") {
            (
                Mp::Map(vec![
                    ("type", Mp::Str("cancelByCloid")),
                    (
                        "cancels",
                        Mp::Array(vec![Mp::Map(vec![
                            ("asset", Mp::UInt(asset as u64)),
                            ("cloid", Mp::Str(order_id)),
                        ])]),
                    ),
                ]),
                serde_json::json!({
                    "type": "cancelByCloid",
                    "cancels": [{"asset": asset, "cloid": order_id}],
                }),
            )
        } else {
            let oid: u64 = order_id
                .parse()
                .map_err(|_| AdapterError::Other(format!("订单号格式不正确：{order_id}")))?;
            (
                Mp::Map(vec![
                    ("type", Mp::Str("cancel")),
                    (
                        "cancels",
                        Mp::Array(vec![Mp::Map(vec![
                            ("a", Mp::UInt(asset as u64)),
                            ("o", Mp::UInt(oid)),
                        ])]),
                    ),
                ]),
                serde_json::json!({
                    "type": "cancel",
                    "cancels": [{"a": asset, "o": oid}],
                }),
            )
        };
        let nonce = NonceAllocator::next(&agent_addr);
        let v = self.post_exchange(action_json, &action_mp, nonce).await?;
        if v["status"].as_str() == Some("err") {
            return Err(AdapterError::Other(format!(
                "交易所拒绝撤单：{}",
                v["response"].as_str().unwrap_or("未知原因")
            )));
        }
        let statuses = v["response"]["data"]["statuses"]
            .as_array()
            .ok_or_else(|| {
                AdapterError::UnknownOutcome("撤单响应中没有撤单状态，撤单结果未知".to_string())
            })?;
        let first = statuses.first().ok_or_else(|| {
            AdapterError::UnknownOutcome("撤单响应中没有撤单状态，撤单结果未知".to_string())
        })?;
        if first.as_str() == Some("success") {
            Ok(())
        } else if let Some(err) = first.get("error").and_then(|e| e.as_str()) {
            // "Order was never placed, already canceled, or filled."
            // is a definite answer about the cancel, but NOT about the
            // order: the caller queries the order to find out which.
            Err(AdapterError::Other(format!("交易所拒绝撤单：{err}")))
        } else {
            Err(AdapterError::UnknownOutcome(
                "撤单响应状态无法识别，撤单结果未知".to_string(),
            ))
        }
    }

    async fn query_order_by_cloid(
        &self,
        _account_id: &str,
        cloid: &str,
    ) -> AdapterResult<OrderInfo> {
        let v = self
            .post_info(serde_json::json!({
                "type": "orderStatus",
                "user": self.address,
                "oid": cloid,
            }))
            .await?;
        // Fill details need the oid + order time; fetch them first so
        // the average price below is real, not the limit price.
        let mut fills_avg = None;
        if let Some(o) = v["order"]["order"].as_object() {
            let oid = o["oid"].as_u64().unwrap_or(0);
            let ts = o["timestamp"].as_i64().unwrap_or(0);
            if oid != 0 && ts > 0 {
                fills_avg = self.fills_avg(oid, ts).await;
            }
        }
        parse_order_status_response(&v, &self.id, &self.account_id, cloid, fills_avg)
            .map_err(AdapterError::Other)
    }

    async fn query_fills(&self, _account_id: &str, cloid: &str) -> AdapterResult<Vec<TradeInfo>> {
        let info = self.query_order_by_cloid(_account_id, cloid).await?;
        let oid: u64 = info
            .order_id
            .parse()
            .map_err(|_| AdapterError::Other("订单号无法解析，无法查询成交".to_string()))?;
        let v = self
            .post_info(serde_json::json!({
                "type": "userFillsByTime",
                "user": self.address,
                "startTime": info.created_at,
                "aggregateByTime": true,
            }))
            .await?;
        Ok(parse_fills(&v, &self.id, &self.account_id, cloid, oid))
    }

    async fn run(&self) {
        // Account sync is REST polling only: no market WS is opened for
        // an account (public market data comes from the on-demand
        // catalog, never from a per-account connection).
        self.rest_loop().await;
    }

    fn wake_sync(&self) {
        self.wake.notify_one();
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

    #[test]
    fn order_status_mapping_never_guesses_a_fill() {
        assert_eq!(map_order_status("open"), OrderStatus::Open);
        assert_eq!(map_order_status("filled"), OrderStatus::Filled);
        assert_eq!(map_order_status("canceled"), OrderStatus::Canceled);
        assert_eq!(map_order_status("marginCanceled"), OrderStatus::Canceled);
        assert_eq!(map_order_status("selfTradeCanceled"), OrderStatus::Canceled);
        assert_eq!(map_order_status("rejected"), OrderStatus::Rejected);
        assert_eq!(map_order_status("tickRejected"), OrderStatus::Rejected);
        assert_eq!(map_order_status("badAloPxRejected"), OrderStatus::Rejected);
        assert_eq!(map_order_status("triggered"), OrderStatus::Open);
        assert_eq!(map_order_status("somethingNew"), OrderStatus::Unresolved);
    }

    #[test]
    fn order_status_response_parses_the_docs_example() {
        // The docs' orderStatus example: a market order, fully filled
        // (sz 0.0 of origSz 0.0076), cloid null.
        let v = serde_json::json!({
            "status": "order",
            "order": {
                "order": {
                    "coin": "ETH", "side": "A", "limitPx": "2412.7",
                    "sz": "0.0", "oid": 1, "timestamp": 1724361546645i64,
                    "triggerCondition": "N/A", "isTrigger": false,
                    "triggerPx": "0.0", "children": [],
                    "isPositionTpsl": false, "reduceOnly": true,
                    "orderType": "Market", "origSz": "0.0076",
                    "tif": "FrontendMarket", "cloid": null
                },
                "status": "filled",
                "statusTimestamp": 1724361546645i64
            }
        });
        let info =
            parse_order_status_response(&v, "hyperliquid", "acc-1", "0xabc", Some(2410.0)).unwrap();
        assert_eq!(info.status, OrderStatus::Filled);
        assert_eq!(info.order_id, "1");
        assert_eq!(info.client_order_id, "0xabc");
        assert_eq!(info.symbol, "ETH");
        assert_eq!(info.side, "sell");
        assert!((info.quantity - 0.0076).abs() < 1e-9);
        assert!((info.filled_quantity - 0.0076).abs() < 1e-9);
        assert!((info.remaining_quantity - 0.0).abs() < 1e-9);
        assert_eq!(info.average_fill_price, Some(2410.0));
        assert_eq!(info.created_at, 1724361546645i64);
    }

    #[test]
    fn order_status_unknown_oid_is_an_error_not_a_state() {
        let v = serde_json::json!({"status": "unknownOid"});
        let err =
            parse_order_status_response(&v, "hyperliquid", "acc-1", "0xabc", None).unwrap_err();
        assert_eq!(err, "unknownOid");
    }

    #[test]
    fn order_status_partial_fill_becomes_partially_filled() {
        let v = serde_json::json!({
            "status": "order",
            "order": {
                "order": {
                    "coin": "io:BTC", "side": "B", "limitPx": "90000.0",
                    "sz": "0.004", "oid": 42, "timestamp": 1724361546645i64,
                    "triggerCondition": "N/A", "isTrigger": false,
                    "triggerPx": "0.0", "children": [],
                    "isPositionTpsl": false, "reduceOnly": false,
                    "orderType": "Limit", "origSz": "0.01",
                    "tif": "Gtc", "cloid": "0x1234567890abcdef1234567890abcdef"
                },
                "status": "open",
                "statusTimestamp": 1724361546700i64
            }
        });
        let info = parse_order_status_response(
            &v,
            "entropy",
            "acc-1",
            "0x1234567890abcdef1234567890abcdef",
            Some(90100.0),
        )
        .unwrap();
        assert_eq!(info.status, OrderStatus::PartiallyFilled);
        assert_eq!(info.symbol, "BTC", "the io: prefix is stripped");
        assert_eq!(info.side, "buy");
        assert!((info.filled_quantity - 0.006).abs() < 1e-9);
        assert!((info.remaining_quantity - 0.004).abs() < 1e-9);
        assert_eq!(info.average_fill_price, Some(90100.0));
        assert_eq!(info.order_type, "limit");
    }

    #[test]
    fn open_orders_parse_the_docs_example() {
        let v = serde_json::json!([{
            "coin": "BTC", "limitPx": "29792.0", "oid": 91490942,
            "side": "A", "sz": "0.0", "timestamp": 1681247412573i64
        }]);
        let orders = parse_open_orders(&v, "hyperliquid", "acc-1").unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order_id, "91490942");
        assert_eq!(orders[0].side, "sell");
        assert_eq!(orders[0].status, OrderStatus::Open);
        assert!((orders[0].price.unwrap() - 29792.0).abs() < 1e-9);
    }

    #[test]
    fn fills_parse_and_match_by_oid() {
        let v = serde_json::json!([
            {"coin": "BTC", "px": "90000.0", "sz": "0.004", "side": "B",
             "time": 1724361546700i64, "oid": 42, "fee": "0.01",
             "feeToken": "USDC", "tid": 111, "hash": "0x00"},
            {"coin": "BTC", "px": "90100.0", "sz": "0.006", "side": "B",
             "time": 1724361546800i64, "oid": 43, "fee": "0.02",
             "feeToken": "USDC", "tid": 112, "hash": "0x00"}
        ]);
        let fills = parse_fills(&v, "hyperliquid", "acc-1", "0xabc", 42);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].trade_id, "111");
        assert_eq!(fills[0].order_id, "42");
        assert_eq!(fills[0].client_order_id, "0xabc");
        assert!((fills[0].price - 90000.0).abs() < 1e-9);
        assert!((fills[0].fee.unwrap() - 0.01).abs() < 1e-9);
        assert_eq!(fills[0].fee_currency.as_deref(), Some("USDC"));
    }

    #[test]
    fn wire_strings_are_decimal_exact() {
        assert_eq!(decimal_to_wire(90000.0).unwrap(), "90000");
        assert_eq!(decimal_to_wire(0.01).unwrap(), "0.01");
        assert_eq!(decimal_to_wire(100.50).unwrap(), "100.5");
        assert!(decimal_to_wire(0.0000000001).is_err());
    }
}
