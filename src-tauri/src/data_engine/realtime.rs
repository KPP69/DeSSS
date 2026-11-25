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
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::adapters::book::BookState;
use crate::adapters::market_catalog::MarketCatalog;
use crate::adapters::now_ms;
use crate::adapters::sync::SyncManager;
use crate::adapters::types::OrderBook;
use crate::bus::{AppEvent, EventBus};
use crate::net::ws::connect_ws;

/// Milliseconds without a frame after which a streamed book counts as
/// stale (the Data page then shows the 2s REST values instead).
pub const STREAM_STALE_MS: i64 = 5_000;

/// One subscribed market: `(exchange_id, symbol)`.
pub type StreamKey = (String, String);

/// Which exchange protocol a stream speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamKind {
    Hyperliquid,
    Lighter,
    Asterdex,
    Arcus,
}

/// Static dial plan for one key, resolved once when the task starts.
/// A plan failure (unknown Lighter market id, unsupported exchange) is
/// permanent: the task logs once and stops instead of retrying forever.
struct Plan {
    kind: StreamKind,
    url: String,
    /// Subscribe message to send after the handshake, if any
    /// (AsterDEX encodes the stream in the URL itself).
    subscribe: Option<String>,
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
                tokio::spawn(async move { this.run_stream(key).await }),
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
            tokio::spawn(async move { this.run_stream(key).await }),
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

    /// Resolve the dial plan for a key. Lighter needs the numeric market
    /// id from the catalog (the row must already be cached — the Data
    /// page only streams symbols its search just returned).
    fn plan(&self, key: &StreamKey) -> Result<Plan, String> {
        let (exchange_id, symbol) = (&key.0, &key.1);
        match exchange_id.as_str() {
            "hyperliquid" => Ok(Plan {
                kind: StreamKind::Hyperliquid,
                url: "wss://api.hyperliquid.xyz/ws".to_string(),
                subscribe: Some(
                    serde_json::json!({
                        "method": "subscribe",
                        "subscription": { "type": "l2Book", "coin": symbol }
                    })
                    .to_string(),
                ),
            }),
            // Entropy is Hyperliquid's HIP-3 dex: same socket, the coin
            // is namespaced `io:<symbol>`.
            "entropy" => Ok(Plan {
                kind: StreamKind::Hyperliquid,
                url: "wss://api.hyperliquid.xyz/ws".to_string(),
                subscribe: Some(
                    serde_json::json!({
                        "method": "subscribe",
                        "subscription": { "type": "l2Book", "coin": format!("io:{symbol}") }
                    })
                    .to_string(),
                ),
            }),
            "lighter" | "lighter_robinhood" => {
                let market_id = self
                    .catalog
                    .market_id(exchange_id, symbol)
                    .ok_or_else(|| format!("目录中没有 {symbol} 的市场编号"))?;
                let base = if exchange_id == "lighter" {
                    "wss://mainnet.zklighter.elliot.ai/stream"
                } else {
                    "wss://api.rh.lighter.xyz/stream"
                };
                Ok(Plan {
                    kind: StreamKind::Lighter,
                    // `?readonly=true` is the documented way to read
                    // public data from a restricted region.
                    url: format!("{base}?readonly=true"),
                    subscribe: Some(
                        serde_json::json!({
                            "type": "subscribe",
                            "channel": format!("order_book/{market_id}")
                        })
                        .to_string(),
                    ),
                })
            }
            "asterdex" => Ok(Plan {
                kind: StreamKind::Asterdex,
                // bookTicker is the top of book only — exactly what the
                // Data page shows — and needs no sequence handling.
                url: format!(
                    "wss://fstream.asterdex.com/ws/{}@bookTicker",
                    symbol.to_lowercase()
                ),
                subscribe: None,
            }),
            "arcus" => Ok(Plan {
                kind: StreamKind::Arcus,
                url: "wss://api.arcus.xyz/v1/ws".to_string(),
                subscribe: Some(
                    serde_json::json!({
                        "type": "subscribe",
                        "channel": "l2Orderbook",
                        "id": symbol,
                        "nLevels": 20
                    })
                    .to_string(),
                ),
            }),
            other => Err(format!("该交易所暂不支持实时流：{other}")),
        }
    }

    /// One stream task: resolve the plan once, then dial and read until
    /// the key is no longer wanted. A failed dial or a dropped socket
    /// retries with backoff (2s, doubling to 30s); a long-lived stream
    /// resets the backoff.
    async fn run_stream(self: Arc<Self>, key: StreamKey) {
        let plan = match self.plan(&key) {
            Ok(p) => p,
            Err(e) => {
                self.bus.log(
                    "error",
                    "stream",
                    format!("{} {} 实时流无法订阅：{e}", key.0, key.1),
                );
                return;
            }
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
                let Some((bids, asks)) = parse_hl_l2book(&v) else {
                    return FrameAction::None;
                };
                book.apply_snapshot(&bids, &asks);
                FrameAction::Publish(book.to_book(&key.0, &key.1, 10, now_ms()))
            }
            StreamKind::Lighter => {
                let Some(frame) = parse_lighter_book_frame(&v) else {
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
                    LighterFrame::Snapshot { nonce, bids, asks } => {
                        book.apply_snapshot_with_nonce(Some(nonce), &bids, &asks);
                        FrameAction::Publish(book.to_book(&key.0, &key.1, 10, now_ms()))
                    }
                    LighterFrame::Delta {
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
                let Some((bid_px, bid_sz, ask_px, ask_sz)) = parse_asterdex_book_ticker(&v) else {
                    return FrameAction::None;
                };
                FrameAction::Publish(OrderBook {
                    adapter_id: key.0.clone(),
                    symbol: key.1.clone(),
                    bids: vec![crate::adapters::types::PriceLevel(bid_px, bid_sz)],
                    asks: vec![crate::adapters::types::PriceLevel(ask_px, ask_sz)],
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
                let Some((bids, asks)) = parse_arcus_l2(&v) else {
                    return FrameAction::None;
                };
                // l2Orderbook sends a full snapshot every ~200ms.
                book.apply_snapshot(&bids, &asks);
                FrameAction::Publish(book.to_book(&key.0, &key.1, 10, now_ms()))
            }
        }
    }
}

/// Hyperliquid `l2Book` frame: `data.levels` is `[bids, asks]`, each
/// level `{"px", "sz", "n"}`. Returns `(bids, asks)` as float pairs.
fn parse_hl_l2book(v: &serde_json::Value) -> Option<(Vec<(f64, f64)>, Vec<(f64, f64)>)> {
    let data = v.get("data")?;
    let levels = data.get("levels")?.as_array()?;
    if levels.len() < 2 {
        return None;
    }
    let parse_side = |side: &serde_json::Value| -> Option<Vec<(f64, f64)>> {
        side.as_array().map(|rows| {
            rows.iter()
                .filter_map(|r| {
                    let px = r.get("px").and_then(|p| p.as_str())?.parse::<f64>().ok()?;
                    let sz = r.get("sz").and_then(|s| s.as_str())?.parse::<f64>().ok()?;
                    Some((px, sz))
                })
                .collect()
        })
    };
    Some((parse_side(&levels[0])?, parse_side(&levels[1])?))
}

/// One parsed Lighter `order_book` frame.
enum LighterFrame {
    /// `subscribed/order_book`: full snapshot + nonce baseline.
    Snapshot {
        nonce: u64,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
    },
    /// `update/order_book`: incremental delta, checked against the
    /// previous frame's nonce.
    Delta {
        begin_nonce: u64,
        nonce: u64,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
    },
}

/// Lighter order_book frame: `type` is `subscribed/order_book` or
/// `update/order_book`; the book lives in `order_book` with string
/// `price` / `size` levels and `nonce` / `begin_nonce`. `offset` is
/// server-scoped and deliberately ignored (see `book.rs`).
fn parse_lighter_book_frame(v: &serde_json::Value) -> Option<LighterFrame> {
    let t = v.get("type").and_then(|t| t.as_str())?;
    let ob = v.get("order_book")?;
    let parse_side = |side: &serde_json::Value| -> Option<Vec<(f64, f64)>> {
        side.as_array().map(|rows| {
            rows.iter()
                .filter_map(|r| {
                    let px = r
                        .get("price")
                        .and_then(|p| p.as_str())?
                        .parse::<f64>()
                        .ok()?;
                    let sz = r
                        .get("size")
                        .and_then(|s| s.as_str())?
                        .parse::<f64>()
                        .ok()?;
                    Some((px, sz))
                })
                .collect()
        })
    };
    let bids = parse_side(ob.get("bids")?)?;
    let asks = parse_side(ob.get("asks")?)?;
    let nonce = ob.get("nonce").and_then(|n| n.as_u64())?;
    match t {
        "subscribed/order_book" => Some(LighterFrame::Snapshot { nonce, bids, asks }),
        "update/order_book" => {
            let begin_nonce = ob.get("begin_nonce").and_then(|n| n.as_u64())?;
            Some(LighterFrame::Delta {
                begin_nonce,
                nonce,
                bids,
                asks,
            })
        }
        _ => None,
    }
}

/// AsterDEX `bookTicker` frame: top of book only.
fn parse_asterdex_book_ticker(v: &serde_json::Value) -> Option<(f64, f64, f64, f64)> {
    if v.get("e").and_then(|e| e.as_str()) != Some("bookTicker") {
        return None;
    }
    let f = |k: &str| v.get(k).and_then(|x| x.as_str())?.parse::<f64>().ok();
    Some((f("b")?, f("B")?, f("a")?, f("A")?))
}

/// Arcus `l2Orderbook` frame: `contents.bids/asks` are `[price, size]`
/// string pairs; every frame is a full snapshot.
fn parse_arcus_l2(v: &serde_json::Value) -> Option<(Vec<(f64, f64)>, Vec<(f64, f64)>)> {
    let contents = v.get("contents")?;
    let parse_side = |side: &serde_json::Value| -> Option<Vec<(f64, f64)>> {
        side.as_array().map(|rows| {
            rows.iter()
                .filter_map(|r| {
                    let pair = r.as_array()?;
                    let px = pair.first().and_then(|p| p.as_str())?.parse::<f64>().ok()?;
                    let sz = pair.get(1).and_then(|s| s.as_str())?.parse::<f64>().ok()?;
                    Some((px, sz))
                })
                .collect()
        })
    };
    Some((
        parse_side(contents.get("bids")?)?,
        parse_side(contents.get("asks")?)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn lighter_snapshot_and_delta() {
        let snap = val(
            r#"{"type":"subscribed/order_book","channel":"order_book:1","order_book":{"asks":[{"price":"100","size":"1"}],"bids":[{"price":"99","size":"2"}],"offset":5,"nonce":10,"begin_nonce":9}}"#,
        );
        match parse_lighter_book_frame(&snap).expect("snapshot") {
            LighterFrame::Snapshot { nonce, bids, asks } => {
                assert_eq!(nonce, 10);
                assert_eq!(bids, vec![(99.0, 2.0)]);
                assert_eq!(asks, vec![(100.0, 1.0)]);
            }
            _ => panic!("expected snapshot"),
        }
        let delta = val(
            r#"{"type":"update/order_book","channel":"order_book:1","order_book":{"asks":[],"bids":[{"price":"99","size":"0"}],"offset":6,"nonce":12,"begin_nonce":10}}"#,
        );
        match parse_lighter_book_frame(&delta).expect("delta") {
            LighterFrame::Delta {
                begin_nonce,
                nonce,
                bids,
                ..
            } => {
                assert_eq!(begin_nonce, 10);
                assert_eq!(nonce, 12);
                assert_eq!(bids, vec![(99.0, 0.0)]);
            }
            _ => panic!("expected delta"),
        }
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
    fn asterdex_book_ticker_is_top_of_book() {
        let v = val(
            r#"{"e":"bookTicker","E":1,"u":1,"s":"BTCUSDT","b":"90000","B":"1.5","a":"90010","A":"0.5"}"#,
        );
        assert_eq!(
            parse_asterdex_book_ticker(&v),
            Some((90000.0, 1.5, 90010.0, 0.5))
        );
        let other = val(r#"{"e":"depthUpdate","s":"BTCUSDT"}"#);
        assert!(parse_asterdex_book_ticker(&other).is_none());
    }

    #[test]
    fn arcus_l2_snapshot_parses() {
        let v = val(
            r#"{"type":"channel_data","channel":"l2Orderbook","id":"BTC-USD","contents":{"bids":[["64822.9","0.85"]],"asks":[["64826.9","0.85"]],"lastSequenceId":1,"globalSequenceId":2,"timestamp":1783750906942677}}"#,
        );
        let (bids, asks) = parse_arcus_l2(&v).expect("book");
        assert_eq!(bids, vec![(64822.9, 0.85)]);
        assert_eq!(asks, vec![(64826.9, 0.85)]);
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
}
