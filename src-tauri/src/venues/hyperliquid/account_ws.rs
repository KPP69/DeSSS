//! Hyperliquid / Entropy account WebSocket (Phase 3.3): one socket
//! per connected account, carrying positions + equity + available +
//! open orders + order updates + fills in real time. REST polling
//! drops to a 60s reconciliation while the socket is healthy and
//! returns to 10s when it degrades.
//!
//! Channel survey (live probes 2026-11-27, frames archived under the
//! goal workspace `evidence/phase33/`). `webData2` — the channel the
//! order names — is DEAD on the live server: subscribing
//! `{"type":"webData2","user":"0x…"}` (twice, with and without
//! `"dex":""`) is rejected with `{"channel":"error","data":"Error
//! parsing JSON into valid websocket request: …"}`. Its successor
//! `webData3` acks, but its position-carrying shape for a non-empty
//! account is unverified (the captured frame for an empty account
//! carries no positions anywhere). The snapshot is therefore taken
//! from the two channels whose frames are byte-shape-identical to
//! the REST responses the parsers already handle:
//! `clearinghouseState` (per dex, positions + margin +
//! withdrawable) and `openOrders` (per dex). `orderUpdates` and
//! `userFills` subscribe exactly as ordered. NO signature is needed
//! for any of them: the official Python SDK's `websocket_manager.py`
//! sends only `{"method":"subscribe","subscription":{…}}` with no
//! key and no signing, and all four subscriptions acked live.
//!
//! The venue pushes a full `clearinghouseState` + `openOrders`
//! snapshot every ~5s even for an idle account (measured 2026-11-27:
//! 1.06s, 4.64s, 9.62s, 14.66s, …). The 3s runner freshness rule
//! therefore cannot read the last-frame time directly — the 1s
//! health tick touches `live.ts` while the socket is healthy, so
//! the 3s rule means "the WS pipeline is alive", not "a frame
//! arrived in the last 3s".
//!
//! First-frame latency (VM, sandbox gateway — not the user's
//! network): dial → first data frame ≈ 1.1s. The app logs the
//! real-machine number on every (re)connect; see
//! `on_clearinghouse_frame`.

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use super::order::{hl_side_to_side, parse_open_orders, parse_order_status_response};
use super::rest::{merge_perp, merge_spot, parse_clearinghouse_state, SpotBalance};
use super::{HyperliquidAdapter, Snapshot};
use crate::core::types::{Order, OrderInfo, TradeInfo};
use crate::core::util::{f64_from, now_ms};
use crate::net::ws::connect_ws;

/// Account-WS endpoint (same as the public market WS).
pub const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

/// Silence after which an account counts as degraded (Phase 3.3 §8).
/// Measured with the monotonic clock (`Instant`), never wall-clock.
pub const DEGRADED_AFTER: Duration = Duration::from_secs(10);

/// Reconnect backoff: 1s, doubling to 30s (Phase 3.3 §4). A session
/// that lived ≥60s resets the backoff — a stable socket that drops
/// once should not wait 30s.
const BACKOFF_START: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Application-level ping, mirroring the official SDK (every 50s).
const PING_INTERVAL: Duration = Duration::from_secs(50);

/// Health tick: degradation check + `live.ts` touch while healthy.
const HEALTH_TICK: Duration = Duration::from_secs(1);

/// Per-dex account snapshots for the WS merge: the
/// `clearinghouseState` channel pushes one dex at a time, and the
/// published snapshot is main + io + spot, exactly like the REST
/// aggregation in `fetch_account_state`.
#[derive(Default)]
pub(crate) struct WsLegs {
    pub(crate) main: Option<Snapshot>,
    pub(crate) io: Option<Snapshot>,
    pub(crate) spot: Option<Vec<SpotBalance>>,
}

/// Account-WS connection state. Starts degraded: before the first WS
/// frame the account is REST-only, and the strategy must not open on
/// REST-only data.
pub(crate) struct WsState {
    /// Socket currently up.
    pub(crate) connected: bool,
    /// Last account data frame received (monotonic clock).
    pub(crate) last_frame: Option<Instant>,
    /// When the current socket was dialed, for first-frame latency.
    pub(crate) connected_at: Option<Instant>,
    /// Dial → first position frame, logged once per connection.
    pub(crate) first_frame_ms: Option<u128>,
    /// True while the account data is REST fallback.
    pub(crate) degraded: bool,
}

impl Default for WsState {
    fn default() -> Self {
        Self {
            connected: false,
            last_frame: None,
            connected_at: None,
            first_frame_ms: None,
            degraded: true,
        }
    }
}

impl WsState {
    /// Degraded at `now`: socket down, or no frame for 10s. Monotonic
    /// clock only — wall-clock jumps never flip it. Pure, so tests can
    /// drive it with constructed `Instant`s.
    pub(crate) fn is_degraded_at(&self, now: Instant) -> bool {
        if !self.connected {
            return true;
        }
        match self.last_frame {
            Some(t) => now.duration_since(t) >= DEGRADED_AFTER,
            None => true,
        }
    }
}

/// One `clearinghouseState` frame: `(dex, snapshot)`. The frame data
/// is `{dex, user, clearinghouseState: {...}}` — the inner object is
/// the REST shape, parsed by the same function.
pub(crate) fn parse_clearinghouse_frame(data: &Value) -> Option<(String, Snapshot)> {
    if data["clearinghouseState"].is_null() {
        return None;
    }
    Some((
        data["dex"].as_str().unwrap_or("").to_string(),
        parse_clearinghouse_state(&data["clearinghouseState"]),
    ))
}

/// One `openOrders` frame: `(dex, orders)`. The frame data is
/// `{dex, user, orders: [...]}` — the array is the REST shape.
pub(crate) fn parse_open_orders_frame(
    data: &Value,
    exchange_id: &str,
    account_id: &str,
) -> Option<(String, Vec<Order>)> {
    let orders = parse_open_orders(&data["orders"], exchange_id, account_id).ok()?;
    Some((data["dex"].as_str().unwrap_or("").to_string(), orders))
}

/// One `orderUpdates` frame: a list of `{order, status,
/// statusTimestamp}` objects (a single object is accepted too — the
/// server sends one applied block per message). Mapped through the
/// same parser as the REST `orderStatus` response. An order the
/// exchange reports without a cloid falls back to its oid, exactly
/// like the open-orders import.
/// One `orderUpdates` frame: a list of `{order, status,
/// statusTimestamp}` items. The WS item is single-nested, but
/// `parse_order_status_response` expects the REST `orderStatus`
/// nesting (`{"order": {"order": {...}, "status": ...}}`), so each
/// item is re-wrapped here. One `OrderInfo` per item that parses.
pub(crate) fn parse_order_updates_frame(
    data: &Value,
    exchange_id: &str,
    account_id: &str,
) -> Vec<OrderInfo> {
    let items: &[Value] = match data.as_array() {
        Some(a) => a,
        None => std::slice::from_ref(data),
    };
    items
        .iter()
        .filter_map(|item| {
            // Fills carry no cloid; the parser falls back to this.
            let cloid = item["order"]["cloid"]
                .as_str()
                .filter(|c| !c.is_empty())
                .map(|c| c.to_string())
                .unwrap_or_else(|| {
                    item["order"]["oid"]
                        .as_u64()
                        .map(|o| o.to_string())
                        .unwrap_or_default()
                });
            let wrapped = serde_json::json!({ "order": item });
            parse_order_status_response(&wrapped, exchange_id, account_id, &cloid, None).ok()
        })
        .collect()
}

/// One fill object from a `userFills` frame, mapped exactly like the
/// REST `userFillsByTime` rows (`parse_fills`). The cloid is resolved
/// by the caller from the store (fills carry no cloid).
fn parse_ws_fill(f: &Value, exchange_id: &str, account_id: &str, cloid: &str) -> Option<TradeInfo> {
    Some(TradeInfo {
        trade_id: f["tid"].as_u64().map(|n| n.to_string()).unwrap_or_default(),
        exchange_id: exchange_id.to_string(),
        order_id: f["oid"].as_u64().map(|n| n.to_string()).unwrap_or_default(),
        client_order_id: cloid.to_string(),
        account_id: account_id.to_string(),
        symbol: f["coin"].as_str().unwrap_or("").to_string(),
        side: hl_side_to_side(f["side"].as_str().unwrap_or("B")),
        price: f64_from(&f["px"]),
        quantity: f64_from(&f["sz"]),
        fee: Some(f64_from(&f["fee"])),
        fee_currency: Some(f["feeToken"].as_str().unwrap_or("USDC").trim().to_string()),
        timestamp: f["time"].as_i64().unwrap_or(0),
    })
}

/// One `userFills` frame: the fills, each with its cloid resolved
/// from the store by oid (our orders), or the oid itself for orders
/// the store never saw (website orders still get recorded).
pub(crate) fn parse_user_fills_frame(
    data: &Value,
    exchange_id: &str,
    account_id: &str,
    cloid_of_oid: &dyn Fn(&str) -> Option<String>,
) -> Vec<TradeInfo> {
    let Some(arr) = data["fills"].as_array() else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|f| {
            let oid = f["oid"].as_u64().map(|n| n.to_string()).unwrap_or_default();
            let cloid = cloid_of_oid(&oid).unwrap_or_else(|| oid.clone());
            parse_ws_fill(f, exchange_id, account_id, &cloid)
        })
        .collect()
}

impl HyperliquidAdapter {
    /// The six subscriptions for one account: the per-dex account
    /// state (main + io) plus the account-wide order and fill feeds.
    /// No signature, no key — the venue's account data is public.
    fn subscriptions(&self) -> [String; 6] {
        let user = &self.address;
        let sub = |t: &str, dex: Option<&str>| {
            let mut s = serde_json::json!({
                "method": "subscribe",
                "subscription": {"type": t, "user": user},
            });
            if let Some(d) = dex {
                s["subscription"]["dex"] = Value::String(d.to_string());
            }
            s.to_string()
        };
        [
            sub("clearinghouseState", Some("")),
            sub("clearinghouseState", Some("io")),
            sub("openOrders", Some("")),
            sub("openOrders", Some("io")),
            sub("orderUpdates", None),
            sub("userFills", None),
        ]
    }

    /// The account-WS run loop: dial, subscribe, read until the
    /// socket drops, then reconnect with backoff (1s → 30s). Exits
    /// when the adapter stops running (disconnect / delete).
    pub(crate) async fn ws_loop(&self) {
        let mut backoff = BACKOFF_START;
        let mut attempt = 0u32;
        while self.running.load(std::sync::atomic::Ordering::SeqCst) {
            let started = Instant::now();
            match self.ws_once(attempt).await {
                Ok(()) => {
                    if !self.running.load(std::sync::atomic::Ordering::SeqCst) {
                        break;
                    }
                    // A clean stream end without a stop request: treat
                    // it as a drop and reconnect.
                    self.log(
                        "warn",
                        format!(
                            "账户 {} 私有 WS 意外结束，{}s 后重连",
                            self.account_id,
                            backoff.as_secs()
                        ),
                    );
                }
                Err(e) => {
                    self.log(
                        "warn",
                        format!(
                            "账户 {} 私有 WS 断开：{e}，{}s 后重连",
                            self.account_id,
                            backoff.as_secs()
                        ),
                    );
                }
            }
            if !self.running.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            self.mark_ws_disconnected();
            attempt += 1;
            if started.elapsed() >= Duration::from_secs(60) {
                backoff = BACKOFF_START;
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
    }

    /// One socket session: dial, REST snapshot, subscribe, read.
    /// The REST snapshot runs BEFORE resubscribing (Phase 3.3 §4):
    /// a reconnect never resumes increments on a drifted base, and
    /// the open-orders snapshot arrives via the WS subscription
    /// itself. Returns Err on any drop; Ok only on a clean stop.
    async fn ws_once(&self, attempt: u32) -> Result<(), String> {
        let (mut ws, _) = connect_ws(WS_URL, Duration::from_secs(10), &self.bus)
            .await
            .map_err(|e| format!("账户 WS 连接失败：{e}"))?;
        {
            let mut st = self.ws_state.lock().unwrap_or_else(|e| e.into_inner());
            st.connected = true;
            st.connected_at = Some(Instant::now());
            st.first_frame_ms = None;
        }
        // Reconnect snapshot first, then the increments.
        self.poll_account().await;
        for sub in self.subscriptions() {
            ws.send(Message::Text(sub.into()))
                .await
                .map_err(|e| format!("账户 WS 订阅失败：{e}"))?;
        }
        if attempt == 0 {
            self.log(
                "info",
                format!("账户 {} 私有 WS 已连接（{WS_URL}）", self.account_id),
            );
        } else {
            self.log(
                "info",
                format!("账户 {} 私有 WS 已重连（第 {attempt} 次）", self.account_id),
            );
        }
        let mut health_tick = tokio::time::interval(HEALTH_TICK);
        health_tick.tick().await;
        let mut ping_tick = tokio::time::interval(PING_INTERVAL);
        ping_tick.tick().await;
        loop {
            tokio::select! {
                msg = ws.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => self.on_ws_text(&text),
                        Some(Ok(Message::Ping(payload))) => {
                            let _ = ws.send(Message::Pong(payload)).await;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => return Err(format!("账户 WS 读取失败：{e}")),
                        None => return Err("账户 WS 已关闭".to_string()),
                    }
                }
                _ = health_tick.tick() => self.ws_health_tick(),
                _ = ping_tick.tick() => {
                    let _ = ws
                        .send(Message::Text("{\"method\":\"ping\"}".to_string().into()))
                        .await;
                }
            }
            if !self.running.load(std::sync::atomic::Ordering::SeqCst) {
                let _ = ws.close(None).await;
                return Ok(());
            }
        }
    }

    /// Dispatch one text frame by channel. Unknown channels are
    /// ignored; a server `error` frame is logged (a rejected
    /// subscription must never fail silently).
    fn on_ws_text(&self, text: &str) {
        let Ok(v) = serde_json::from_str::<Value>(text) else {
            return;
        };
        let recv = Instant::now();
        match v["channel"].as_str().unwrap_or("") {
            "clearinghouseState" => self.on_clearinghouse_frame(&v["data"], recv),
            "openOrders" => self.on_open_orders_frame(&v["data"]),
            "orderUpdates" => self.on_order_updates_frame(&v["data"]),
            "userFills" => self.on_user_fills_frame(&v["data"]),
            "error" => {
                self.log(
                    "error",
                    format!("账户 {} 私有 WS 报错：{}", self.account_id, v["data"]),
                );
            }
            _ => {}
        }
    }

    /// A `clearinghouseState` frame replaces one dex leg wholesale
    /// (full snapshot, never a delta). Both legs must have arrived
    /// at least once before anything publishes — until then the
    /// merged snapshot would silently drop a dex. The REST poll
    /// normally fills the legs first, so this only delays the first
    /// WS publish by one frame at most.
    fn on_clearinghouse_frame(&self, data: &Value, recv: Instant) {
        let Some((dex, snap)) = parse_clearinghouse_frame(data) else {
            return;
        };
        if !dex.is_empty() && dex != "io" {
            return;
        }
        let ready = {
            let mut legs = self.ws_legs.lock().unwrap_or_else(|e| e.into_inner());
            match dex.as_str() {
                "" => legs.main = Some(snap),
                _ => legs.io = Some(snap),
            }
            legs.main.is_some() && legs.io.is_some()
        };
        self.note_ws_frame();
        if !ready {
            return;
        }
        let merged = {
            let legs = self.ws_legs.lock().unwrap_or_else(|e| e.into_inner());
            let mut m = merge_perp(
                legs.main.clone().unwrap_or_default(),
                legs.io.clone().unwrap_or_default(),
            );
            if let Some(spot) = &legs.spot {
                m = merge_spot(m, spot, &|_| None);
            }
            m
        };
        *self.snapshot.lock().unwrap_or_else(|e| e.into_inner()) = merged;
        self.publish_live("ws");
        // First-frame instrumentation (Phase 3.3 §17): dial → frame
        // plus frame → published position, logged once per connection.
        let mut st = self.ws_state.lock().unwrap_or_else(|e| e.into_inner());
        if st.first_frame_ms.is_none() {
            let connect_ms = st
                .connected_at
                .map(|t| recv.duration_since(t).as_millis())
                .unwrap_or(0);
            st.first_frame_ms = Some(connect_ms);
            let apply_ms = recv.elapsed().as_millis();
            drop(st);
            self.log(
                "info",
                format!(
                    "账户 {} 私有 WS 首帧：连接后 {connect_ms}ms，帧到持仓 {apply_ms}ms",
                    self.account_id
                ),
            );
        }
    }

    /// An `openOrders` frame replaces the store's open list for this
    /// account (same merge as the REST sync). Orders missing from the
    /// frame are NOT reconciled here — the 60s REST sync owns that;
    /// reconciling on every 5s frame would spam REST queries.
    fn on_open_orders_frame(&self, data: &Value) {
        let Some((dex, orders)) = parse_open_orders_frame(data, &self.id, &self.account_id) else {
            return;
        };
        if !dex.is_empty() && dex != "io" {
            return;
        }
        self.note_ws_frame();
        let _ = self.orders.merge_open_orders(&self.account_id, &orders);
    }

    /// An `orderUpdates` frame applies each order status to the store
    /// — this is the execution layer's main confirmation path (Phase
    /// 3.3 §5). The Trading page polls the store, so the update shows
    /// there on its next refresh.
    fn on_order_updates_frame(&self, data: &Value) {
        let updates = parse_order_updates_frame(data, &self.id, &self.account_id);
        if updates.is_empty() {
            return;
        }
        self.note_ws_frame();
        for info in &updates {
            self.orders.apply_query(info);
        }
    }

    /// A `userFills` frame appends its fills (deduped by trade id).
    /// It does NOT touch positions: while the WS is healthy the
    /// `clearinghouseState` snapshot carries every fill, and applying
    /// the same fill twice would double-count (Phase 3.3 §6). The
    /// local estimate (`SyncManager::apply_fill`) covers the degraded
    /// case, where no snapshot is coming.
    fn on_user_fills_frame(&self, data: &Value) {
        let fills = parse_user_fills_frame(data, &self.id, &self.account_id, &|oid| {
            self.orders.get_by_oid(oid).map(|o| o.client_order_id)
        });
        if fills.is_empty() {
            return;
        }
        self.note_ws_frame();
        // Group by cloid: one append per order keeps the store write
        // to one lock per order, not one per fill.
        let mut by_cloid: Vec<(String, Vec<TradeInfo>)> = Vec::new();
        for f in fills {
            match by_cloid.iter_mut().find(|(c, _)| *c == f.client_order_id) {
                Some((_, v)) => v.push(f),
                None => by_cloid.push((f.client_order_id.clone(), vec![f])),
            }
        }
        for (cloid, v) in by_cloid {
            self.orders.append_fills(&cloid, v);
        }
    }

    /// One account data frame arrived: mark the socket live, clear
    /// degradation (logging the recovery once), and stamp the
    /// monotonic last-frame time. Called by every frame handler.
    fn note_ws_frame(&self) {
        let mut st = self.ws_state.lock().unwrap_or_else(|e| e.into_inner());
        let was_degraded = st.degraded;
        st.last_frame = Some(Instant::now());
        st.connected = true;
        if was_degraded {
            st.degraded = false;
            drop(st);
            self.log(
                "info",
                format!("账户 {} 数据恢复：WS 已恢复", self.account_id),
            );
        }
    }

    /// Socket down: mark it and degrade at once (Phase 3.3 §8 — a
    /// dead socket is degraded immediately, not after 10s).
    fn mark_ws_disconnected(&self) {
        {
            let mut st = self.ws_state.lock().unwrap_or_else(|e| e.into_inner());
            st.connected = false;
            st.connected_at = None;
        }
        self.set_ws_degraded("WS 已断开");
    }

    /// Flip to degraded, logging + republishing exactly once per
    /// transition. The republish carries source "rest" so the UI
    /// shows the fallback line at once.
    fn set_ws_degraded(&self, reason: &str) {
        let mut st = self.ws_state.lock().unwrap_or_else(|e| e.into_inner());
        if st.degraded {
            return;
        }
        st.degraded = true;
        drop(st);
        self.log(
            "warn",
            format!(
                "账户 {} 数据降级：{reason}，切换 REST 兜底",
                self.account_id
            ),
        );
        self.publish_live("rest");
    }

    /// 1s health tick: silence past 10s degrades; while healthy the
    /// live timestamp is touched so the runner's 3s freshness rule
    /// reads "the WS pipeline is alive" (the venue pushes every ~5s,
    /// so a raw last-frame age would flap past 3s).
    fn ws_health_tick(&self) {
        let now = Instant::now();
        let degraded_now = self
            .ws_state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_degraded_at(now);
        if degraded_now {
            self.set_ws_degraded("WS 10 秒无帧");
        } else {
            self.touch_live_ts();
        }
    }

    /// Refresh the backend live timestamp without a bus publish: the
    /// runner reads it from the backend map, and the frontend never
    /// uses `ts`.
    fn touch_live_ts(&self) {
        if let Ok(mut map) = self.live.write() {
            if let Some(entry) = map.get_mut(&self.account_id) {
                entry.ts = now_ms();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::EventBus;
    use crate::core::types::OrderStatus;
    use crate::market::reconcile::ReconcileState;
    use crate::store::orders::OrderStore;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    /// Real `clearinghouseState` envelope, captured live 2026-11-27
    /// from wss://api.hyperliquid.xyz/ws for
    /// 0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a (an empty account):
    /// the frame shape is verbatim; the two positions are rows in the
    /// documented `assetPositions` shape (the same shape the REST
    /// parser already consumes), with margin/withdrawable set to
    /// match. The io leg is the same envelope with dex "io" and no
    /// positions.
    fn main_clearinghouse_data() -> Value {
        serde_json::from_str(
            r#"{"dex":"","user":"0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a","clearinghouseState":{"marginSummary":{"accountValue":"15000.0","totalNtlPos":"14900.0","totalRawUsd":"0.0","totalMarginUsed":"0.0"},"crossMarginSummary":{"accountValue":"15000.0","totalNtlPos":"14900.0","totalRawUsd":"0.0","totalMarginUsed":"0.0"},"crossMaintenanceMarginUsed":"0.0","withdrawable":"8000.0","assetPositions":[{"position":{"coin":"BTC","szi":"0.1","entryPx":"90000.0","positionValue":"9100.0","unrealizedPnl":"100.0"},"type":"oneWay"},{"position":{"coin":"ETH","szi":"-2.0","entryPx":"3000.0","positionValue":"5800.0","unrealizedPnl":"200.0"},"type":"oneWay"}],"time":1795766763068}}"#,
        )
        .expect("fixture json")
    }

    fn io_clearinghouse_data() -> Value {
        serde_json::from_str(
            r#"{"dex":"io","user":"0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a","clearinghouseState":{"marginSummary":{"accountValue":"200.0","totalNtlPos":"0.0","totalRawUsd":"0.0","totalMarginUsed":"0.0"},"crossMarginSummary":{"accountValue":"200.0","totalNtlPos":"0.0","totalRawUsd":"0.0","totalMarginUsed":"0.0"},"crossMaintenanceMarginUsed":"0.0","withdrawable":"150.0","assetPositions":[],"time":1795766763068}}"#,
        )
        .expect("fixture json")
    }

    /// Real `openOrders` envelope (captured live, same session):
    /// shape verbatim, three orders in the documented row shape.
    fn open_orders_data() -> Value {
        serde_json::from_str(
            r#"{"dex":"","user":"0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a","orders":[{"coin":"BTC","side":"B","limitPx":"90000.0","sz":"0.01","oid":1001,"timestamp":1795766763000,"origSz":"0.01","cloid":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","orderType":"Limit","isTrigger":false,"reduceOnly":false},{"coin":"ETH","side":"A","limitPx":"3000.0","sz":"1.0","oid":1002,"timestamp":1795766763001,"origSz":"1.0","cloid":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","orderType":"Limit","isTrigger":false,"reduceOnly":false},{"coin":"BTC","side":"A","limitPx":"91000.0","sz":"0.01","oid":1003,"timestamp":1795766763002,"origSz":"0.01","cloid":"0xcccccccccccccccccccccccccccccccc","orderType":"Take Profit Market","isTrigger":true,"triggerPx":"91000.0","reduceOnly":true}]}"#,
        )
        .expect("fixture json")
    }

    /// One `orderUpdates` FILLED item. No live data frame was
    /// captured (no order activity during the probe window), so the
    /// shape is the documented `{order, status, statusTimestamp}`
    /// block — the same shape the REST `orderStatus` parser consumes.
    fn order_updates_filled() -> Value {
        serde_json::from_str(
            r#"[{"order":{"coin":"BTC","side":"B","limitPx":"90000.0","sz":"0.0","oid":1001,"timestamp":1795766763000,"origSz":"0.01","cloid":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","orderType":"Limit","tif":"Gtc"},"status":"filled","statusTimestamp":1795766764000}]"#,
        )
        .expect("fixture json")
    }

    /// Real `userFills` snapshot, captured live 2026-11-27
    /// (isSnapshot=true, two fills for the test address) — verbatim.
    fn user_fills_data() -> Value {
        serde_json::from_str(
            r#"{"isSnapshot":true,"user":"0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a","fills":[{"coin":"ETH","px":"1909.6","sz":"0.0157","side":"B","time":1785978068530,"startPosition":"0.0","dir":"Open Long","closedPnl":"0.0","hash":"0x79b7f863e289095d7b31044189d50d02023f00497d8c282f1d80a3b6a18ce348","oid":511087664983,"crossed":true,"fee":"0.013491","tid":632037799211354,"feeToken":"USDC","twapId":null},{"coin":"ETH","px":"1853.7","sz":"0.0157","side":"A","time":1786462934423,"startPosition":"0.0157","dir":"Close Long","closedPnl":"-0.87763","hash":"0xb3b5965126ff0608b52f0441f17686010100ae36c1f224da577e41a3e5f2dff3","oid":514591959746,"crossed":true,"fee":"0.012572","tid":472987449843967,"feeToken":"USDC","twapId":null}]}"#,
        )
        .expect("fixture json")
    }

    fn adapter() -> (
        Arc<HyperliquidAdapter>,
        Arc<RwLock<HashMap<String, crate::core::types::AccountLive>>>,
        Arc<OrderStore>,
    ) {
        let live = Arc::new(RwLock::new(HashMap::new()));
        let orders = Arc::new(OrderStore::new());
        let a = Arc::new(HyperliquidAdapter::new(
            "hyperliquid",
            None,
            "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a".to_string(),
            "acc-1".to_string(),
            EventBus::new(16),
            live.clone(),
            Arc::new(ReconcileState::new()),
            None,
            orders.clone(),
        ));
        (a, live, orders)
    }

    fn live_of(
        live: &Arc<RwLock<HashMap<String, crate::core::types::AccountLive>>>,
    ) -> crate::core::types::AccountLive {
        live.read()
            .unwrap()
            .get("acc-1")
            .expect("live published")
            .clone()
    }

    #[test]
    fn clearinghouse_frame_replaces_positions_without_accumulating() {
        let (a, live, _) = adapter();
        a.on_clearinghouse_frame(&main_clearinghouse_data(), Instant::now());
        // The io leg has not arrived yet: nothing publishes — a
        // half-merged snapshot would silently drop a dex.
        assert!(live.read().unwrap().get("acc-1").is_none());
        a.on_clearinghouse_frame(&io_clearinghouse_data(), Instant::now());
        let l = live_of(&live);
        assert_eq!(l.source, "ws");
        assert!(!l.degraded);
        assert_eq!(l.positions.len(), 2);
        assert!((l.equity_usd - 15200.0).abs() < 1e-9);
        assert!((l.available_usd - 8150.0).abs() < 1e-9);
        assert!((l.pnl_usd.unwrap() - 300.0).abs() < 1e-9);
        let btc = l.positions.iter().find(|p| p.symbol == "BTC").unwrap();
        assert_eq!(btc.side, "long");
        assert!((btc.qty - 0.1).abs() < 1e-12);
        assert!((btc.mark_price - 91000.0).abs() < 1e-9);
        let eth = l.positions.iter().find(|p| p.symbol == "ETH").unwrap();
        assert_eq!(eth.side, "short");
        assert!((eth.qty - 2.0).abs() < 1e-12);
        assert!((eth.mark_price - 2900.0).abs() < 1e-9);
        // The same frame again: still 2 positions, same equity —
        // whole replacement, never an accumulation.
        a.on_clearinghouse_frame(&main_clearinghouse_data(), Instant::now());
        let l = live_of(&live);
        assert_eq!(l.positions.len(), 2);
        assert!((l.equity_usd - 15200.0).abs() < 1e-9);
    }

    #[test]
    fn fill_then_snapshot_does_not_double_count() {
        let (a, live, orders) = adapter();
        a.on_clearinghouse_frame(&main_clearinghouse_data(), Instant::now());
        a.on_clearinghouse_frame(&io_clearinghouse_data(), Instant::now());
        assert_eq!(live_of(&live).positions.len(), 2);
        // A real fill arrives: it is recorded, but the position is
        // NOT touched — the snapshot carries it (§6).
        a.on_user_fills_frame(&user_fills_data());
        assert_eq!(orders.list_fills().len(), 2);
        assert_eq!(orders.list_fills()[0].symbol, "ETH");
        // The fill did NOT move the position: the snapshot's ETH
        // short (2.0 @ 3000) is still exactly as the exchange said.
        let l = live_of(&live);
        assert_eq!(l.positions.len(), 2);
        let eth = l.positions.iter().find(|p| p.symbol == "ETH").unwrap();
        assert_eq!(eth.side, "short");
        assert!((eth.qty - 2.0).abs() < 1e-12);
        assert!((eth.entry_price - 3000.0).abs() < 1e-12);
        // The next snapshot still shows exactly the exchange state.
        a.on_clearinghouse_frame(&main_clearinghouse_data(), Instant::now());
        assert_eq!(live_of(&live).positions.len(), 2);
    }

    #[test]
    fn open_orders_frame_merges_into_store() {
        let (a, _, orders) = adapter();
        a.on_open_orders_frame(&open_orders_data());
        let open = orders.list_open();
        assert_eq!(open.len(), 3);
        let tp = open.iter().find(|o| o.order_id == "1003").unwrap();
        assert_eq!(tp.tpsl.as_deref(), Some("tp"));
        assert_eq!(tp.trigger_px, Some(91000.0));
        assert_eq!(tp.order_type, "trigger");
        // The same frame again: no duplicate rows.
        a.on_open_orders_frame(&open_orders_data());
        assert_eq!(orders.list_open().len(), 3);
    }

    #[test]
    fn order_updates_filled_maps_to_order_info() {
        let infos = parse_order_updates_frame(&order_updates_filled(), "hyperliquid", "acc-1");
        assert_eq!(infos.len(), 1);
        let info = &infos[0];
        assert_eq!(info.status, OrderStatus::Filled);
        assert_eq!(info.symbol, "BTC");
        assert_eq!(info.side, "buy");
        assert!((info.filled_quantity - 0.01).abs() < 1e-12);
        assert_eq!(info.average_fill_price, Some(90000.0));
        assert_eq!(info.client_order_id, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    }

    #[test]
    fn user_fills_frame_maps_the_real_captured_fill() {
        let fills = parse_user_fills_frame(&user_fills_data(), "hyperliquid", "acc-1", &|_| None);
        assert_eq!(fills.len(), 2);
        let f = &fills[0];
        assert_eq!(f.symbol, "ETH");
        assert_eq!(f.side, "buy");
        assert!((f.price - 1909.6).abs() < 1e-9);
        assert!((f.quantity - 0.0157).abs() < 1e-12);
        assert_eq!(f.trade_id, "632037799211354");
        assert_eq!(f.order_id, "511087664983");
        assert!((f.fee.unwrap() - 0.013491).abs() < 1e-9);
        assert_eq!(f.fee_currency.as_deref(), Some("USDC"));
    }

    #[test]
    fn degraded_after_10s_of_silence_and_recovers_on_a_frame() {
        let mut st = WsState::default();
        // Starts degraded: REST-only until the first frame.
        assert!(st.degraded);
        assert!(st.is_degraded_at(Instant::now()));
        let now = Instant::now();
        st.connected = true;
        st.last_frame = Some(now);
        assert!(!st.is_degraded_at(now));
        assert!(!st.is_degraded_at(now + Duration::from_secs(9)));
        assert!(st.is_degraded_at(now + Duration::from_secs(10)));
        assert!(st.is_degraded_at(now + Duration::from_secs(11)));
        st.connected = false;
        assert!(st.is_degraded_at(now));
    }

    #[test]
    fn subscriptions_carry_no_signature_and_cover_both_dexes() {
        let (a, _, _) = adapter();
        let subs: Vec<Value> = a
            .subscriptions()
            .iter()
            .map(|s| serde_json::from_str(s).expect("valid json"))
            .collect();
        assert_eq!(subs.len(), 6);
        let types: Vec<&str> = subs
            .iter()
            .map(|s| s["subscription"]["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            types,
            [
                "clearinghouseState",
                "clearinghouseState",
                "openOrders",
                "openOrders",
                "orderUpdates",
                "userFills"
            ]
        );
        for s in &subs {
            assert_eq!(s["method"], "subscribe");
            assert!(s["subscription"].get("signature").is_none());
            assert!(s["subscription"].get("apiKey").is_none());
            assert_eq!(
                s["subscription"]["user"],
                "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a"
            );
        }
        assert_eq!(subs[0]["subscription"]["dex"], "");
        assert_eq!(subs[1]["subscription"]["dex"], "io");
    }
}
