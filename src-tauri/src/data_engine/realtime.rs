//! On-demand per-symbol WebSocket streams (`RealtimeEngine`).
//!
//! The task book calls this manager "DataEngine"; that name is already
//! taken by the SQLite engine in `data_engine/mod.rs`, so this module is
//! `RealtimeEngine` and lives beside it instead.
//!
//! Rules:
//! - No demand, no connection: a stream exists only while its
//!   `(exchange_id, symbol)` pair is in the wanted set (the Data page
//!   reconciles the set from the search hits on screen). Clearing the
//!   query, leaving the page, or hiding a row removes the key and its
//!   task is aborted at once — no socket lingers for an unseen symbol.
//! - One task per key, one socket per task. A dropped connection retries
//!   with backoff while the key is still wanted; a task that is no
//!   longer wanted exits instead of reconnecting.
//! - Every accepted frame updates the book's freshness timestamp; a book
//!   with no update for [`STREAM_STALE_MS`] is stale. The Data page
//!   falls back to the 2s REST tick for stale rows, and the risk gateway
//!   (phase 5A §3) refuses orders on stale books.
//! - Streams are read-only market data. Nothing here ever sends an
//!   order, so the read-only retry rule does not apply — and no retry
//!   helper from `net` is used for writes anywhere else either.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tauri::async_runtime::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::bus::{AppEvent, EventBus};
use crate::core::book::BookState;
use crate::core::stream_kind::StreamKind;
use crate::core::types::OrderBook;
use crate::core::util::now_ms;
use crate::market::catalog::MarketCatalog;
use crate::market::sync::SyncManager;
use crate::net::ws::connect_ws;

/// Milliseconds without a frame after which a streamed book counts as
/// stale (the Data page then shows the 2s REST values instead).
pub const STREAM_STALE_MS: i64 = 5_000;

/// One subscribed market: `(exchange_id, symbol)`.
pub type StreamKey = (String, String);

/// Static dial plan for one key, resolved once when the task starts.
/// A plan failure is transient (missing catalog data — retried, see
/// `resolve_plan_with_retry`) or permanent (unknown exchange — the
/// task logs once and stops).
#[derive(Debug, PartialEq, Eq)]
struct Plan {
    kind: StreamKind,
    url: String,
    /// Subscribe message to send after the handshake, if any
    /// (AsterDEX encodes the stream in the URL itself).
    subscribe: Option<String>,
}

/// Why a dial plan could not be built: transient (missing
/// catalog data — the catalog refresh fills it, so retry) vs
/// permanent (unknown exchange — the registry will never know
/// it, so stop). A venue's own stream error is always transient:
/// the only way `Venue::stream` fails today is a missing catalog
/// market id.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PlanError {
    Transient(String),
    Permanent(String),
}

/// What one parsed frame asks the stream loop to do.
enum FrameAction {
    /// Publish this book.
    Publish(OrderBook),
    /// The local book has a gap (Lighter nonce): clear it and subscribe
    /// again for a fresh snapshot.
    Resubscribe,
    /// Control / error / unrelated frame: nothing to publish.
    None,
}

/// One `(exchange_id, symbol)` pair from the `set_stream_symbols`
/// command, exactly as the frontend sends it (`src/lib/tauri.ts`).
/// Lives here — not in `commands/` — so the serde alignment test below
/// runs in the scratch crate. The `rename_all` is load-bearing: without
/// it serde expects `exchange_id` while the frontend sends `exchangeId`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamKeyInput {
    pub exchange_id: String,
    pub symbol: String,
}

pub struct RealtimeEngine {
    bus: EventBus,
    catalog: Arc<MarketCatalog>,
    wanted: Mutex<HashSet<StreamKey>>,
    tasks: Mutex<HashMap<StreamKey, JoinHandle<()>>>,
    /// Last accepted frame per key, in wall-clock ms.
    last_update: RwLock<HashMap<StreamKey, i64>>,
    /// Position reconciliation hook (spec §3): when a stream drops and
    /// later reconnects, the accounts of that exchange are reconciled.
    /// None in tests, where no SyncManager exists.
    sync: Option<Arc<SyncManager>>,
    /// Keys whose socket ended at least once: on the next successful
    /// dial they trigger the reconciliation above.
    dropped: Mutex<HashSet<StreamKey>>,
}

impl RealtimeEngine {
    pub fn new(bus: EventBus, catalog: Arc<MarketCatalog>, sync: Option<Arc<SyncManager>>) -> Self {
        Self {
            bus,
            catalog,
            wanted: Mutex::new(HashSet::new()),
            tasks: Mutex::new(HashMap::new()),
            last_update: RwLock::new(HashMap::new()),
            sync,
            dropped: Mutex::new(HashSet::new()),
        }
    }

    /// Reconcile the wanted set with the keys on screen. New keys spawn
    /// a stream task; removed keys have their task aborted at once and
    /// their freshness entry dropped. Repeating the same set is a no-op.
    pub fn set_wanted(self: &Arc<Self>, keys: Vec<StreamKey>) {
        let wanted: HashSet<StreamKey> = keys.into_iter().collect();
        let mut cur = self.wanted.lock().unwrap_or_else(|e| e.into_inner());
        let removed: Vec<StreamKey> = cur.difference(&wanted).cloned().collect();
        let added: Vec<StreamKey> = wanted.difference(&cur).cloned().collect();
        *cur = wanted;
        drop(cur);
        if removed.is_empty() && added.is_empty() {
            return;
        }
        let mut tasks = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        for key in removed {
            if let Some(handle) = tasks.remove(&key) {
                handle.abort();
            }
            if let Ok(mut m) = self.last_update.write() {
                m.remove(&key);
            }
        }
        for key in added {
            let this = Arc::clone(self);
            tasks.insert(
                key.clone(),
                tauri::async_runtime::spawn(async move { this.run_stream(key).await }),
            );
        }
    }

    pub fn is_wanted(&self, key: &StreamKey) -> bool {
        self.wanted.lock().map(|m| m.contains(key)).unwrap_or(false)
    }

    /// Add one key to the wanted set (spawning its stream task when
    /// new). Additive only — unlike `set_wanted` it never removes
    /// anything, so the order path can make sure the traded symbol's
    /// book is live without disturbing the Data page's set.
    pub fn ensure_wanted(self: &Arc<Self>, key: StreamKey) {
        let mut cur = self.wanted.lock().unwrap_or_else(|e| e.into_inner());
        if !cur.insert(key.clone()) {
            return;
        }
        drop(cur);
        let mut tasks = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        let this = Arc::clone(self);
        tasks.insert(
            key.clone(),
            tauri::async_runtime::spawn(async move { this.run_stream(key).await }),
        );
    }

    /// True when the key has no stream or its last frame is older than
    /// [`STREAM_STALE_MS`]. Used by the Data page fallback and by the
    /// risk gateway: never trade on a stale book.
    pub fn is_stale(&self, key: &StreamKey) -> bool {
        match self.last_update_at(key) {
            Some(ts) => now_ms() - ts > STREAM_STALE_MS,
            None => true,
        }
    }

    pub fn last_update_at(&self, key: &StreamKey) -> Option<i64> {
        self.last_update
            .read()
            .ok()
            .and_then(|m| m.get(key).copied())
    }

    /// Wait for the first frame of `key`, up to `timeout` (F6:
    /// cold-start guard for place_order — a freshly subscribed
    /// stream has no frame yet). Returns false on timeout; never
    /// touches `STREAM_STALE_MS` or `is_stale`.
    pub async fn wait_for_first_frame(&self, key: &StreamKey, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while self.last_update_at(key).is_none() {
            if start.elapsed() >= timeout {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        true
    }

    /// Resolve the dial plan for a key. Lighter needs the numeric market
    /// id from the catalog (the row must already be cached — the Data
    /// page only streams symbols its search just returned).
    fn plan(&self, key: &StreamKey) -> Result<Plan, PlanError> {
        let (exchange_id, symbol) = (&key.0, &key.1);
        let Some(v) = crate::venues::all()
            .iter()
            .find(|v| v.id() == exchange_id.as_str())
        else {
            return Err(PlanError::Permanent(format!("未知交易所：{exchange_id}")));
        };
        let sp = v
            .stream(symbol, self.catalog.market_id(exchange_id, symbol))
            .map_err(PlanError::Transient)?;
        Ok(Plan {
            kind: sp.kind,
            url: sp.url,
            subscribe: sp.subscribe,
        })
    }

    /// Resolve the dial plan, retrying transient failures (missing
    /// catalog data) every 30s or immediately on `CatalogUpdated`,
    /// whichever first. Permanent failures (unknown exchange) log
    /// once and stop. Returns `None` when the plan can never be
    /// built or the key is no longer wanted. Logs only when the
    /// error changes or when a retry finally succeeds; retries are
    /// uncapped, and a failed plan holds no socket or task (only
    /// the plan is re-resolved — dialing starts after it succeeds).
    async fn resolve_plan_with_retry(self: &Arc<Self>, key: &StreamKey) -> Option<Plan> {
        let mut last_err: Option<String> = None;
        let mut rx = self.bus.subscribe();
        loop {
            match self.plan(key) {
                Ok(p) => {
                    if last_err.is_some() {
                        self.bus.log(
                            "info",
                            "stream",
                            format!("{} {} 实时流订阅恢复", key.0, key.1),
                        );
                    }
                    return Some(p);
                }
                Err(PlanError::Permanent(e)) => {
                    self.bus.log(
                        "error",
                        "stream",
                        format!("{} {} 实时流无法订阅：{e}", key.0, key.1),
                    );
                    return None;
                }
                Err(PlanError::Transient(e)) => {
                    if last_err.as_deref() != Some(e.as_str()) {
                        self.bus.log(
                            "warn",
                            "stream",
                            format!("{} {} 实时流暂无订阅计划，30s 后重试：{e}", key.0, key.1),
                        );
                        last_err = Some(e);
                    }
                    // Wait 30s or until the catalog updates, whichever
                    // first. A lagged receiver means events were missed,
                    // so retry now; a closed channel (shutdown) waits
                    // out the 30s instead of spinning.
                    let update = async {
                        loop {
                            match rx.recv().await {
                                Ok(AppEvent::CatalogUpdated) => break,
                                Ok(_) => continue,
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => break,
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                    tokio::time::sleep(Duration::from_secs(30)).await;
                                    break;
                                }
                            }
                        }
                    };
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                        _ = update => {}
                    }
                    if !self.is_wanted(key) {
                        return None;
                    }
                }
            }
        }
    }

    /// One stream task: resolve the plan once, then dial and read until
    /// the key is no longer wanted. A failed dial or a dropped socket
    /// retries with backoff (2s, doubling to 30s); a long-lived stream
    /// resets the backoff.
    async fn run_stream(self: Arc<Self>, key: StreamKey) {
        let Some(plan) = self.resolve_plan_with_retry(&key).await else {
            return;
        };
        let mut backoff = Duration::from_secs(2);
        loop {
            let started = std::time::Instant::now();
            match self.stream_once(&key, &plan).await {
                Ok(()) => {
                    self.bus.log(
                        "warn",
                        "stream",
                        format!(
                            "{} {} 实时流断开，{}s 后重连",
                            key.0,
                            key.1,
                            backoff.as_secs()
                        ),
                    );
                }
                Err(e) => {
                    self.bus.log(
                        "warn",
                        "stream",
                        format!(
                            "{} {} 实时流断开：{e}，{}s 后重连",
                            key.0,
                            key.1,
                            backoff.as_secs()
                        ),
                    );
                }
            }
            // The socket ended: the next successful dial reconciles the
            // positions of this exchange (spec §3, WS 断线恢复后对账).
            if let Ok(mut d) = self.dropped.lock() {
                d.insert(key.clone());
            }
            if started.elapsed() > Duration::from_secs(30) {
                backoff = Duration::from_secs(2);
            }
            tokio::time::sleep(backoff).await;
            if !self.is_wanted(&key) {
                return;
            }
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }

    /// Dial once and read frames until the socket ends. Returns when the
    /// connection drops; the caller decides whether to reconnect.
    async fn stream_once(&self, key: &StreamKey, plan: &Plan) -> Result<(), String> {
        let (stream, _resp) = connect_ws(&plan.url, Duration::from_secs(10), &self.bus).await?;
        // Reconnected after a drop: reconcile this exchange's positions
        // against the exchange (the book stream is back, the account
        // state may have moved while it was down).
        let was_dropped = self
            .dropped
            .lock()
            .map(|mut d| d.remove(key))
            .unwrap_or(false);
        if was_dropped {
            if let Some(sync) = &self.sync {
                sync.reconcile_exchange(&key.0);
            }
        }
        let (mut write, mut read) = stream.split();
        if let Some(sub) = &plan.subscribe {
            write
                .send(Message::Text(sub.clone().into()))
                .await
                .map_err(|e| format!("订阅发送失败：{e}"))?;
        }
        self.bus.log(
            "info",
            "stream",
            format!("{} {} 实时流已连接", key.0, key.1),
        );
        let mut book = BookState::new();
        // Keepalive: at least one outbound frame every 2 minutes or the
        // server closes the socket (Lighter's documented rule; the
        // others tolerate it too). A ping frame is not an app message.
        let mut ping = tokio::time::interval(Duration::from_secs(60));
        ping.tick().await;
        loop {
            tokio::select! {
                _ = ping.tick() => {
                    if write.send(Message::Ping(Default::default())).await.is_err() {
                        return Err("心跳发送失败".to_string());
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            match self.handle_frame(key, plan.kind, &mut book, text.as_str()) {
                                FrameAction::Publish(b) => self.publish(key, b),
                                FrameAction::Resubscribe => {
                                    book.clear();
                                    if let Some(sub) = &plan.subscribe {
                                        let _ = write
                                            .send(Message::Text(sub.clone().into()))
                                            .await;
                                    }
                                }
                                FrameAction::None => {}
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            let _ = write.send(Message::Pong(payload)).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            return Err("服务器关闭连接".to_string())
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => return Err(format!("读取失败：{e}")),
                        None => return Err("连接已关闭".to_string()),
                    }
                }
            }
        }
    }

    /// Record a fresh book: freshness timestamp + publish to the bus
    /// (the forwarder emits it as `market://book`; the bus also caches
    /// it for the runtime snapshot).
    fn publish(&self, key: &StreamKey, book: OrderBook) {
        if let Ok(mut m) = self.last_update.write() {
            m.insert(key.clone(), book.ts);
        }
        self.bus.publish(AppEvent::Book(book));
    }

    /// Test-only seam: record a book as if a frame just arrived, so
    /// `wait_for_first_frame` / `is_stale` see it without a live
    /// stream. Production code never calls this.
    #[cfg(test)]
    pub fn inject_book(&self, key: &StreamKey, book: OrderBook) {
        self.publish(key, book);
    }

    /// Parse one text frame for the stream's exchange. Pure except for
    /// the warning logs on gaps / degraded snapshots.
    fn handle_frame(
        &self,
        key: &StreamKey,
        kind: StreamKind,
        book: &mut BookState,
        text: &str,
    ) -> FrameAction {
        let v: serde_json::Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return FrameAction::None,
        };
        match kind {
            StreamKind::Hyperliquid => {
                if v.get("channel").and_then(|c| c.as_str()) != Some("l2Book") {
                    return FrameAction::None;
                }
                let Some((bids, asks)) = crate::venues::hyperliquid::stream::parse_hl_l2book(&v)
                else {
                    return FrameAction::None;
                };
                book.apply_snapshot(&bids, &asks);
                FrameAction::Publish(book.to_book(&key.0, &key.1, 10, now_ms()))
            }
            StreamKind::Lighter => {
                let Some(frame) = crate::venues::lighter::stream::parse_lighter_book_frame(&v)
                else {
                    if let Some(err) = v.get("error") {
                        self.bus.log(
                            "warn",
                            "stream",
                            format!("{} {} 实时流错误：{err}", key.0, key.1),
                        );
                    }
                    return FrameAction::None;
                };
                match frame {
                    crate::venues::lighter::stream::LighterFrame::Snapshot {
                        nonce,
                        bids,
                        asks,
                    } => {
                        book.apply_snapshot_with_nonce(Some(nonce), &bids, &asks);
                        FrameAction::Publish(book.to_book(&key.0, &key.1, 10, now_ms()))
                    }
                    crate::venues::lighter::stream::LighterFrame::Delta {
                        begin_nonce,
                        nonce,
                        bids,
                        asks,
                    } => match book.apply_delta(&bids, &asks, Some(begin_nonce), Some(nonce)) {
                        Ok(()) => FrameAction::Publish(book.to_book(&key.0, &key.1, 10, now_ms())),
                        Err(gap) => {
                            self.bus.log(
                                "warn",
                                "stream",
                                format!("{} {} 盘口跳号（{gap}），重新订阅", key.0, key.1),
                            );
                            FrameAction::Resubscribe
                        }
                    },
                }
            }
            StreamKind::Asterdex => {
                let Some((bid_px, bid_sz, ask_px, ask_sz)) =
                    crate::venues::asterdex::stream::parse_asterdex_book_ticker(&v)
                else {
                    return FrameAction::None;
                };
                FrameAction::Publish(OrderBook {
                    adapter_id: key.0.clone(),
                    symbol: key.1.clone(),
                    bids: vec![crate::core::types::PriceLevel(bid_px, bid_sz)],
                    asks: vec![crate::core::types::PriceLevel(ask_px, ask_sz)],
                    ts: now_ms(),
                })
            }
            StreamKind::Arcus => {
                let t = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
                if t == "degraded" {
                    self.bus.log(
                        "warn",
                        "stream",
                        format!(
                            "{} {} 实时流快照暂不可用（{}），等待恢复",
                            key.0,
                            key.1,
                            v.get("reason").and_then(|r| r.as_str()).unwrap_or("")
                        ),
                    );
                    return FrameAction::None;
                }
                if t != "subscribed" && t != "channel_data" {
                    return FrameAction::None;
                }
                if v.get("channel").and_then(|c| c.as_str()) != Some("l2Orderbook") {
                    return FrameAction::None;
                }
                let Some((bids, asks)) = crate::venues::arcus::stream::parse_arcus_l2(&v) else {
                    return FrameAction::None;
                };
                // l2Orderbook sends a full snapshot every ~200ms.
                book.apply_snapshot(&bids, &asks);
                FrameAction::Publish(book.to_book(&key.0, &key.1, 10, now_ms()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::market::MarketInfo;
    use crate::venues::hyperliquid::stream::parse_hl_l2book;

    fn val(s: &str) -> serde_json::Value {
        serde_json::from_str(s).expect("fixture")
    }

    #[test]
    fn hl_l2book_parses_levels() {
        let v = val(
            r#"{"channel":"l2Book","data":{"coin":"BTC","levels":[[{"px":"90000","sz":"1.5","n":2}],[{"px":"90010","sz":"0.5","n":1}]],"time":1}}"#,
        );
        let (bids, asks) = parse_hl_l2book(&v).expect("book");
        assert_eq!(bids, vec![(90000.0, 1.5)]);
        assert_eq!(asks, vec![(90010.0, 0.5)]);
    }

    #[test]
    fn hl_ignores_other_channels() {
        let v = val(r#"{"channel":"trades","data":{}}"#);
        assert!(parse_hl_l2book(&v).is_none());
    }

    #[test]
    fn lighter_delta_applies_and_gap_resubscribes() {
        let bus = EventBus::new(16);
        let catalog = Arc::new(MarketCatalog::new(bus.clone()));
        let engine = RealtimeEngine::new(bus, catalog, None);
        let key: StreamKey = ("lighter".to_string(), "BTC-USDC".to_string());
        let mut book = BookState::new();
        let snap = r#"{"type":"subscribed/order_book","channel":"order_book:1","order_book":{"asks":[{"price":"100","size":"1"}],"bids":[{"price":"99","size":"2"}],"offset":5,"nonce":10,"begin_nonce":9}}"#;
        assert!(matches!(
            engine.handle_frame(&key, StreamKind::Lighter, &mut book, snap),
            FrameAction::Publish(_)
        ));
        // Gap: begin_nonce 11 != last nonce 10 → resubscribe.
        let gap = r#"{"type":"update/order_book","channel":"order_book:1","order_book":{"asks":[],"bids":[],"offset":6,"nonce":12,"begin_nonce":11}}"#;
        assert!(matches!(
            engine.handle_frame(&key, StreamKind::Lighter, &mut book, gap),
            FrameAction::Resubscribe
        ));
        // Clean delta after the resubscribe baseline (nonce 10 again):
        // the gap frame did not move the baseline, so begin 10 applies.
        let ok = r#"{"type":"update/order_book","channel":"order_book:1","order_book":{"asks":[],"bids":[{"price":"99","size":"3"}],"offset":7,"nonce":13,"begin_nonce":10}}"#;
        assert!(matches!(
            engine.handle_frame(&key, StreamKind::Lighter, &mut book, ok),
            FrameAction::Publish(_)
        ));
    }

    #[test]
    fn arcus_degraded_is_not_a_book() {
        let bus = EventBus::new(16);
        let catalog = Arc::new(MarketCatalog::new(bus.clone()));
        let engine = RealtimeEngine::new(bus, catalog, None);
        let key: StreamKey = ("arcus".to_string(), "BTC-USD".to_string());
        let mut book = BookState::new();
        let degraded = r#"{"type":"degraded","channel":"l2Orderbook","id":"BTC-USD","reason":"snapshot_stale","retryAfterMs":500}"#;
        assert!(matches!(
            engine.handle_frame(&key, StreamKind::Arcus, &mut book, degraded),
            FrameAction::None
        ));
        assert!(book.is_empty());
    }

    #[test]
    fn stale_without_any_frame() {
        let bus = EventBus::new(16);
        let catalog = Arc::new(MarketCatalog::new(bus.clone()));
        let engine = RealtimeEngine::new(bus, catalog, None);
        let key: StreamKey = ("hyperliquid".to_string(), "BTC".to_string());
        assert!(engine.is_stale(&key));
        assert_eq!(engine.last_update_at(&key), None);
    }

    /// Frontend/backend field alignment: the exact JSON `tauri.ts`
    /// `setStreamSymbols` sends must deserialize (same class of bug as
    /// the 2026-11-26 `place_order` "missing field `account_id`").
    #[test]
    fn stream_key_input_matches_frontend_payload() {
        let keys: Vec<StreamKeyInput> = serde_json::from_str(
            r#"[{"exchangeId":"hyperliquid","symbol":"BTC"},{"exchangeId":"entropy","symbol":"ETH"}]"#,
        )
        .expect("frontend payload must deserialize");
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].exchange_id, "hyperliquid");
        assert_eq!(keys[0].symbol, "BTC");
        assert_eq!(keys[1].exchange_id, "entropy");
    }

    /// Rule 2.2: the catalog symbol goes out verbatim — no case
    /// change, no prefix added, no prefix stripped. The Entropy row
    /// is the regression guard: the catalog already stores `io:NVDA`,
    /// so the old `io:{symbol}` prefixing subscribed to the
    /// nonexistent `io:io:NVDA` and the stream silently never
    /// produced a book (masked by the REST fallback).
    #[test]
    fn plan_keeps_the_catalog_symbol_verbatim() {
        let bus = EventBus::new(16);
        let catalog = Arc::new(MarketCatalog::new(bus.clone()));
        let engine = RealtimeEngine::new(bus, catalog, None);
        // (exchange, symbol, coin as sent on the wire)
        for (exchange_id, symbol, want) in [
            ("hyperliquid", "BTC", "BTC"),
            ("entropy", "io:NVDA", "io:NVDA"),
            ("arcus", "BTC-USD", "BTC-USD"),
        ] {
            let key: StreamKey = (exchange_id.to_string(), symbol.to_string());
            let plan = engine.plan(&key).expect("plan");
            let sub: serde_json::Value =
                serde_json::from_str(plan.subscribe.as_ref().expect("subscribe")).expect("json");
            let coin = match exchange_id {
                "arcus" => sub["id"].as_str().unwrap(),
                _ => sub["subscription"]["coin"].as_str().unwrap(),
            };
            assert_eq!(coin, want, "{exchange_id} {symbol}");
        }
        // AsterDEX encodes the stream in the URL itself.
        let key: StreamKey = ("asterdex".to_string(), "BTCUSDT".to_string());
        let plan = engine.plan(&key).expect("plan");
        assert!(plan.subscribe.is_none());
        assert_eq!(plan.url, "wss://fstream.asterdex.com/ws/btcusdt@bookTicker");
        // Lighter needs the catalog market id; with an empty catalog
        // the lookup fails on the verbatim symbol — never a rewritten
        // one.
        for exchange_id in ["lighter", "lighter_robinhood"] {
            let key: StreamKey = (exchange_id.to_string(), "BTC-USDC".to_string());
            let err = engine.plan(&key).expect_err("no catalog rows");
            assert_eq!(
                err,
                PlanError::Transient("目录中没有 BTC-USDC 的市场编号".to_string())
            );
        }
    }

    /// F6: the wait expires when no frame ever lands, and returns
    /// at once once a frame timestamp exists. (The timestamp is
    /// seeded directly; frame parsing is covered by the
    /// `handle_frame` tests.)
    #[tokio::test]
    async fn wait_for_first_frame_waits_then_returns() {
        let bus = EventBus::new(16);
        let catalog = Arc::new(MarketCatalog::new(bus.clone()));
        let engine = RealtimeEngine::new(bus, catalog, None);
        let key: StreamKey = ("hyperliquid".to_string(), "BTC".to_string());
        assert!(
            !engine
                .wait_for_first_frame(&key, Duration::from_millis(150))
                .await
        );
        if let Ok(mut m) = engine.last_update.write() {
            m.insert(key.clone(), now_ms());
        }
        assert!(
            engine
                .wait_for_first_frame(&key, Duration::from_secs(2))
                .await
        );
    }

    #[test]
    fn plan_error_is_transient_for_missing_market_id_and_permanent_for_unknown_exchange() {
        let bus = EventBus::new(16);
        let catalog = Arc::new(MarketCatalog::new(bus.clone()));
        let engine = RealtimeEngine::new(bus, catalog, None);
        // Lighter with an empty catalog: no market id -> transient.
        let key: StreamKey = ("lighter".to_string(), "BTC-USDC".to_string());
        assert_eq!(
            engine.plan(&key),
            Err(PlanError::Transient(
                "目录中没有 BTC-USDC 的市场编号".to_string()
            ))
        );
        // An exchange the registry never heard of: permanent.
        let key: StreamKey = ("nope".to_string(), "BTC".to_string());
        assert_eq!(
            engine.plan(&key),
            Err(PlanError::Permanent("未知交易所：nope".to_string()))
        );
    }

    #[test]
    fn plan_succeeds_once_the_catalog_row_arrives() {
        let bus = EventBus::new(16);
        let catalog = Arc::new(MarketCatalog::new(bus.clone()));
        let engine = RealtimeEngine::new(bus, catalog.clone(), None);
        let row = MarketInfo {
            exchange_id: "lighter".to_string(),
            symbol: "BTC-USDC".to_string(),
            base_symbol: "BTC".to_string(),
            market_id: Some(1),
            settlement_currency: "USDC".to_string(),
            max_leverage: None,
            tick_size: None,
            step_size: None,
            min_notional: None,
            bid_px: None,
            ask_px: None,
            mid_px: None,
            mark_px: None,
            index_px: None,
            funding_rate_raw: None,
            funding_interval_hours: None,
            funding_1h: None,
            funding_apr: None,
            next_funding_time: None,
            volume_24h_usd: None,
            open_interest_usd: None,
            updated_at: 0,
        };
        catalog.store("lighter", vec![row]);
        let key: StreamKey = ("lighter".to_string(), "BTC-USDC".to_string());
        let plan = engine.plan(&key).expect("plan");
        assert_eq!(
            plan.url,
            "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"
        );
        let sub: serde_json::Value =
            serde_json::from_str(plan.subscribe.as_ref().expect("subscribe")).expect("json");
        assert_eq!(sub["channel"].as_str().unwrap(), "order_book/1");
    }
}
