//! Bulk market catalog (phase 4): contract specs + live prices + funding rates
//! for all six exchanges, standardized into [`MarketInfo`] and served to the
//! Data page's symbol search.
//!
//! Search is zero-network (PR-0.3): static fields live in SQLite
//! `instruments` as [`Instrument`] and load into memory at startup, so
//! the first search after a restart reads local data only; live fields
//! ([`Quote`]) stay in memory and come from bulk refreshes, WS frames,
//! or the one-time 5s-staleness REST fallback for the top rows. The
//! catalog refreshes in the background (startup, every 12h, manual,
//! or a search/order miss) — never on the search path. A search
//! therefore makes zero requests; a failed exchange backs off 60s and
//! its last good rows stay in place.
//!
//! Design notes (all endpoints probed live 2026-11-25/26, no keys needed):
//! - Hyperliquid / Entropy: one `POST metaAndAssetCtxs` each (weight 20).
//!   Funding is the **hourly** rate; tick size is derived from price level
//!   and `szDecimals`; the bulk response carries NO bid/ask (its
//!   `impactPxs` is an execution-price proxy, never top-of-book) — the
//!   true BBO comes only from the per-hit `POST l2Book` fill (coin =
//!   native symbol, `io:NVDA` works for Entropy); a failed fill leaves
//!   bid/ask `None`, never a proxy number.
//! - Lighter / Lighter RH: `GET orderBookDetails` (all markets) +
//!   `GET funding-rates` (filter `exchange == "lighter"`). The funding rate
//!   is an **8h-equivalent quote** (settlement itself is hourly); divide by
//!   8 for the hourly rate. No bid/ask in bulk REST — the BBO is filled
//!   per search hit via `GET orderBookOrders?market_id=..&limit=2`.
//! - AsterDEX: `exchangeInfo` (specs, refreshed at most once an hour —
//!   890 KB) + `premiumIndex` + `bookTicker` + `fundingInfo` +
//!   `ticker/24hr` (all symbols, one call) every cycle. Funding intervals
//!   are **per-symbol** (`fundingInfo` live data: 360 symbols at 4h, 333
//!   at 8h, 105 at 1h, 3 at 2h — never assume 8h). When `fundingInfo`
//!   fails or a symbol is missing from it, the interval is **unknown**:
//!   `funding_interval_hours`, `funding_1h` and `funding_apr` all stay
//!   `None` (the frontend shows "—") — no 8h default, no invented APR.
//!   24h volume comes from `ticker/24hr` (`quoteVolume`). Open interest
//!   is always `None`: the V3 spec has **no openInterest endpoint** (the
//!   string does not appear in it at all), so nothing is fetched and
//!   nothing is faked — the frontend shows "—". `max_leverage` has **no
//!   public source** (exchangeInfo carries no leverage field; `maintMarginPercent` 2.5% /
//!   `requiredMarginPercent` 5% are margin ratios, not max leverage —
//!   Binance lists identical values for BTCUSDT whose max is 125x), so
//!   it stays null. The catalog holds no account credentials at all:
//!   public market data never depends on any account's lifecycle.
//! - Arcus: `GET /v1/markets` alone covers specs, prices and funding.
//!   Funding is already the **hourly** rate. No bid/ask in bulk REST —
//!   the BBO is filled per search hit via `GET /v1/bbo/{market}`
//!   (e.g. `BTC-USD`).
//!   (docs.arcus.xyz/api-reference/openapi.yml is 404, as is /v1/mid-prices;
//!   the per-market funding-rates endpoint is not bulk-usable.)
//! - Liquidity: Hyperliquid `dayNtlVlm` + `openInterest * markPx`,
//!   Lighter `daily_quote_token_volume` + `open_interest_usd` (direct),
//!   Arcus `volume24hNotional` + `openInterest * markPrice`,
//!   AsterDEX `ticker/24hr quoteVolume` + open interest always `None`
//!   (no V3 openInterest endpoint exists).
//! - Search matches `base_symbol` exactly or by prefix, or the native
//!   `symbol` by substring (case insensitive) — e.g. "BT" finds BTC but a
//!   mid-string fragment of a base name alone does not.
//! - Funding APR is a simple multiple everywhere: `funding_1h * 24 * 365`.
//! - Refresh failures are logged to the console as warnings (never errors,
//!   so they don't light the red dot) and only when the message changes,
//!   so a persistently failing exchange doesn't spam the console. The last
//!   good data for that exchange stays in the cache.
//! - Concurrency: each exchange has its own async `Mutex`, so at most one
//!   refresh (bulk or live) per exchange runs at a time while different
//!   exchanges still refresh concurrently; per-market enrichment (BBO /
//!   OI fills) is capped at [`ENRICH_CONCURRENCY`] concurrent requests
//!   by a `Semaphore`, so a wide search cannot trigger 429s.
//! - Two halves: the static catalog ([`Instrument`]: specs, leverage,
//!   tick sizes) persists in SQLite and keeps the 30s bulk cache; the
//!   live half ([`Quote`]: mark, funding, BBO) is memory-only and
//!   arrives with each bulk refresh, from WS frames on the Data page,
//!   or from the one-time 5s-staleness REST fallback for a search's
//!   top 5 rows. A watched search re-reads memory every 2s — zero
//!   requests — and re-searches once when a background refresh
//!   completes (`CatalogUpdated`).
//! - `last_success_at` records the last cycle in which at least one
//!   exchange actually succeeded — attempt time is never recorded, so a
//!   fully failed cycle leaves the "缓存更新于" timestamp untouched.
//! - This module is Tauri-free, so it runs in the scratch test crate.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::bus::EventBus;
use crate::core::market::{Instrument, MarketInfo, Quote};
use crate::net::shared_http_client;

/// Cached rows are fresh for 30s: a search inside that window serves the
/// cache with no request; older rows are refetched on demand.
pub const CACHE_TTL: Duration = Duration::from_secs(30);
/// One exchange's bulk fetch may take at most this long: a slow venue
/// fails into backoff instead of holding a search hostage. The shared
/// HTTP client's 20s timeout is untouched (a global bound, not a
/// per-venue one).
pub const REFRESH_TIMEOUT: Duration = Duration::from_secs(5);
/// After a failed refresh, automatic refreshes skip the exchange for
/// this long (manual refresh bypasses it). One failure never retries
/// on every keystroke.
pub const ERROR_BACKOFF: Duration = Duration::from_secs(60);
/// The WS-staleness fallback covers at most this many of a search's
/// top rows, once, in the background.
pub const FALLBACK_TOP_N: usize = 5;
/// Max concurrent per-market enrichment requests (BBO / OI fills): wide
/// searches fan out over many symbols, and the venues rate-limit (429)
/// above roughly a dozen parallel calls.
const ENRICH_CONCURRENCY: usize = 10;

/// Outcome of one exchange's refresh attempt, for the `refresh`
/// instrumentation line: skipped (fresh or backed off — not a
/// failure), stored (new rows), or failed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RefreshOutcome {
    Skipped,
    Stored,
    Failed,
}

pub struct MarketCatalog {
    http: reqwest::Client,
    bus: EventBus,
    /// SQLite handle for the `instruments` table. `None` in tests and
    /// in any catalog built without a database (memory-only, no
    /// persistence); `Some` in the app via [`MarketCatalog::with_db`].
    db: Option<Arc<crate::data_engine::DataEngine>>,
    data: RwLock<HashMap<(String, String), MarketInfo>>,
    last_errors: Mutex<HashMap<String, String>>,
    /// When each exchange last failed a refresh (unix `Instant`):
    /// automatic refreshes skip it inside [`ERROR_BACKOFF`].
    error_at: Mutex<HashMap<String, Instant>>,
    /// Last time the 5s-staleness fallback was claimed, to keep the
    /// 2s search tick from spawning one fallback task per tick.
    last_fallback: Mutex<Option<Instant>>,
    /// When each exchange's rows were last fetched successfully.
    fetched_at: Mutex<HashMap<String, Instant>>,
    /// Last time any exchange actually succeeded in a refresh cycle
    /// (unix ms) — the "缓存更新于" timestamp. Attempt time is never
    /// recorded: a cycle where every exchange failed leaves this
    /// untouched.
    last_success_at: Mutex<Option<i64>>,
    /// One async mutex per exchange: at most one refresh (bulk or live)
    /// per exchange runs at a time, while different exchanges still run
    /// concurrently. The registry lock itself is held only to get or
    /// insert the entry, never across an await.
    refresh_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Caps concurrent per-market enrichment requests (BBO / OI fills).
    enrich_sem: tokio::sync::Semaphore,
}

impl MarketCatalog {
    pub fn new(bus: EventBus) -> Self {
        Self {
            http: shared_http_client(),
            bus,
            db: None,
            data: RwLock::new(HashMap::new()),
            last_errors: Mutex::new(HashMap::new()),
            error_at: Mutex::new(HashMap::new()),
            last_fallback: Mutex::new(None),
            fetched_at: Mutex::new(HashMap::new()),
            last_success_at: Mutex::new(None),
            refresh_locks: Mutex::new(HashMap::new()),
            enrich_sem: tokio::sync::Semaphore::new(ENRICH_CONCURRENCY),
        }
    }

    /// The app's catalog: like [`MarketCatalog::new`] but with the
    /// `instruments` table attached, so a cold start loads the last
    /// stored catalog with no network. A load failure is logged and
    /// the catalog starts empty (a background refresh refills it).
    pub fn with_db(bus: EventBus, db: Arc<crate::data_engine::DataEngine>) -> Self {
        let catalog = Self {
            db: Some(db.clone()),
            ..Self::new(bus)
        };
        match db.load_instruments() {
            Ok(instruments) => {
                let mut data = catalog.data.write().unwrap_or_else(|e| e.into_inner());
                for ins in &instruments {
                    let row = MarketInfo::assemble(ins, &Quote::default());
                    data.insert((row.exchange_id.clone(), row.symbol.clone()), row);
                }
                tracing::info!(
                    "catalog loaded {} instruments from sqlite, network=0",
                    instruments.len()
                );
            }
            Err(e) => tracing::warn!("catalog sqlite load failed: {e}"),
        }
        catalog
    }

    /// The per-exchange refresh lock (created on first use). Two tasks
    /// refreshing the same exchange serialize on it; different exchanges
    /// use different locks and proceed concurrently.
    fn refresh_lock(&self, exchange_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut registry = self.refresh_locks.lock().unwrap_or_else(|e| e.into_inner());
        registry
            .entry(exchange_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// True when the exchange's cached rows are still inside the TTL.
    fn is_fresh(&self, exchange_id: &str) -> bool {
        self.fetched_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(exchange_id)
            .map(|t| t.elapsed() < CACHE_TTL)
            .unwrap_or(false)
    }

    /// True when the exchange has cached catalog rows. The order path
    /// uses this to detect a cold start: a user who never visited the
    /// Data page has an empty catalog, and ordering must not fail with
    /// "不在品种目录中" before even trying to load it.
    pub fn has_rows_for(&self, exchange_id: &str) -> bool {
        let data = self.data.read().unwrap_or_else(|e| e.into_inner());
        data.values().any(|row| row.exchange_id == exchange_id)
    }

    /// The latest refresh failure for one exchange, if its most recent
    /// attempt failed (a success clears it). Lets callers — and tests —
    /// tell "a load was attempted but failed" apart from "never tried".
    pub fn last_refresh_error(&self, exchange_id: &str) -> Option<String> {
        self.last_errors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(exchange_id)
            .cloned()
    }

    /// Load this one exchange's catalog when it has no cached rows yet
    /// (cold start). Other exchanges are never touched. Returns true
    /// when rows are available afterwards — already loaded, or the fetch
    /// just succeeded. False means the fetch failed; the caller then
    /// reports the symbol as missing, with the failure already logged
    /// to the console by the refresh itself.
    pub async fn ensure_exchange_loaded(&self, exchange_id: &str) -> bool {
        if self.has_rows_for(exchange_id) {
            return true;
        }
        self.refresh_exchanges(&[exchange_id.to_string()], false)
            .await;
        self.has_rows_for(exchange_id)
    }

    /// Log a refresh failure as a console warning, but only when the
    /// message changed since the last failure for this `key` — a
    /// persistently failing exchange must not spam the console on every
    /// search. `key` is usually the exchange id; the AsterDEX
    /// fundingInfo fetch has its own key so its failure doesn't spam or
    /// get cleared by the main AsterDEX refresh.
    fn log_refresh_error(&self, key: &str, message: String) {
        let mut last = self.last_errors.lock().unwrap_or_else(|e| e.into_inner());
        let changed = last.get(key).map(|m| m.as_str()) != Some(message.as_str());
        if changed {
            last.insert(key.to_string(), message.clone());
            self.bus.log("warn", "市场数据", message);
        }
    }

    fn log_refresh_ok(&self, exchange_id: &str) {
        let mut last = self.last_errors.lock().unwrap_or_else(|e| e.into_inner());
        last.remove(exchange_id);
    }

    /// Replace one exchange's rows in memory, persist their static
    /// halves to SQLite, and log the symbol diff. Quotes are never
    /// persisted. A first load logs the whole list (capped at 10
    /// symbols per side); later cycles log only what changed.
    pub(crate) fn store(&self, exchange_id: &str, rows: Vec<MarketInfo>) {
        let old: std::collections::HashSet<String> = {
            let data = self.data.read().unwrap_or_else(|e| e.into_inner());
            data.values()
                .filter(|r| r.exchange_id == exchange_id)
                .map(|r| r.symbol.clone())
                .collect()
        };
        let new: std::collections::HashSet<String> =
            rows.iter().map(|r| r.symbol.clone()).collect();
        let mut added: Vec<&str> = new.difference(&old).map(|s| s.as_str()).collect();
        let mut removed: Vec<&str> = old.difference(&new).map(|s| s.as_str()).collect();
        added.sort_unstable();
        removed.sort_unstable();
        let instruments: Vec<Instrument> = rows.iter().map(|r| r.split().0).collect();
        if let Some(db) = &self.db {
            if let Err(e) = db.replace_instruments(exchange_id, &instruments) {
                tracing::warn!("catalog sqlite persist failed ({exchange_id}): {e}");
            }
        }
        {
            let mut data = self.data.write().unwrap_or_else(|e| e.into_inner());
            data.retain(|(ex, _), _| ex != exchange_id);
            for row in rows {
                data.insert((row.exchange_id.clone(), row.symbol.clone()), row);
            }
        }
        if !added.is_empty() || !removed.is_empty() {
            tracing::info!(
                "catalog venue={exchange_id} added=[{}] removed=[{}]",
                Self::diff_list(&added),
                Self::diff_list(&removed),
            );
        }
    }

    /// A diff side for the log: up to 10 symbols, then `…+N more`.
    fn diff_list(items: &[&str]) -> String {
        let shown: Vec<&str> = items.iter().take(10).copied().collect();
        let mut out = shown.join(", ");
        if items.len() > 10 {
            if !out.is_empty() {
                out.push_str(", ");
            }
            out.push_str(&format!("…+{} more", items.len() - 10));
        }
        out
    }

    /// Per-venue catalog status for the System page and the refresh
    /// button: row count and the newest row's `updated_at` (the
    /// "目录更新于" time — a failed cycle leaves it untouched).
    pub fn venue_status(&self, exchange_id: &str) -> (usize, Option<i64>) {
        let data = self.data.read().unwrap_or_else(|e| e.into_inner());
        let mut rows = 0;
        let mut updated_at = None;
        for row in data.values().filter(|r| r.exchange_id == exchange_id) {
            rows += 1;
            updated_at = Some(updated_at.unwrap_or(0).max(row.updated_at));
        }
        (rows, updated_at)
    }

    fn mark_fetched(&self, exchange_id: &str) {
        self.fetched_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(exchange_id.to_string(), Instant::now());
    }

    /// Store freshly fetched rows and mark the exchange fetched (success
    /// time only — a failed fetch never refreshes the TTL). Returns true
    /// on success.
    fn apply(&self, exchange_id: &str, result: Result<Vec<MarketInfo>, String>) -> bool {
        match result {
            Ok(rows) => {
                self.log_refresh_ok(exchange_id);
                self.error_at
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(exchange_id);
                self.store(exchange_id, rows);
                self.mark_fetched(exchange_id);
                true
            }
            Err(e) => {
                self.error_at
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(exchange_id.to_string(), Instant::now());
                self.log_refresh_error(exchange_id, format!("{exchange_id} 全量行情刷新失败：{e}"));
                false
            }
        }
    }

    /// True when the exchange failed a refresh inside [`ERROR_BACKOFF`]:
    /// automatic refreshes skip it (manual refresh bypasses this).
    fn in_backoff(&self, exchange_id: &str) -> bool {
        self.error_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(exchange_id)
            .map(|t| t.elapsed() < ERROR_BACKOFF)
            .unwrap_or(false)
    }

    /// Pure refresh gate (no network): fresh (inside [`CACHE_TTL`])
    /// or backed off (failed inside [`ERROR_BACKOFF`]) means skip;
    /// `force` (manual refresh) overrides both. Unit-tested without
    /// network.
    fn should_refresh(&self, exchange_id: &str, force: bool) -> bool {
        if force {
            return true;
        }
        if self.is_fresh(exchange_id) {
            return false;
        }
        if self.in_backoff(exchange_id) {
            return false;
        }
        true
    }

    /// Fetch one exchange's bulk rows under that exchange's refresh lock,
    /// so two concurrent refreshes of the same exchange serialize while
    /// different exchanges still run concurrently. Freshness is re-checked
    /// under the lock: a concurrent refresh may have just finished while
    /// this task was waiting. The whole per-exchange fetch runs under a
    /// [`REFRESH_TIMEOUT`] timeout, so one slow venue can never hold a
    /// search hostage; a timeout is recorded as a failure (60s backoff).
    async fn refresh_one(&self, exchange_id: &str, force: bool) -> RefreshOutcome {
        let lock = self.refresh_lock(exchange_id);
        let _guard = lock.lock().await;
        if !self.should_refresh(exchange_id, force) {
            return RefreshOutcome::Skipped;
        }
        let started = Instant::now();
        let fetched = async {
            let Some(v) = crate::venues::all().iter().find(|v| v.id() == exchange_id) else {
                return false;
            };
            self.apply(exchange_id, v.catalog(&self.http).await)
        };
        let ok = match tokio::time::timeout(REFRESH_TIMEOUT, fetched).await {
            Ok(ok) => ok,
            Err(_) => self.apply(exchange_id, Err("刷新超时（5s）".to_string())),
        };
        let rows = if ok {
            self.venue_status(exchange_id).0
        } else {
            0
        };
        tracing::info!(
            "catalog venue={exchange_id} took={}ms ok={ok} rows={rows}",
            started.elapsed().as_millis()
        );
        if ok {
            RefreshOutcome::Stored
        } else {
            RefreshOutcome::Failed
        }
    }

    /// Fetch (concurrently) the requested exchanges whose cache is stale
    /// (older than [`CACHE_TTL`]) — or all of them when `force` is set
    /// (manual refresh). Each exchange refreshes under its own lock, so
    /// concurrent callers cannot run two fetches for the same exchange.
    /// A single exchange failing leaves its last good rows in place and
    /// logs a warning. Unknown ids are ignored. `last_success_at` moves
    /// only when at least one exchange actually succeeded — and the
    /// page is told to re-search once via `CatalogUpdated`.
    pub async fn refresh_exchanges(&self, exchange_ids: &[String], force: bool) {
        use futures_util::future::join_all;
        let started = Instant::now();
        let wanted: Vec<&str> = crate::venues::all()
            .iter()
            .map(|v| v.id())
            .filter(|id| exchange_ids.iter().any(|x| x == id))
            .collect();
        let todo: Vec<&str> = wanted
            .into_iter()
            .filter(|id| force || !self.is_fresh(id))
            .collect();
        if todo.is_empty() {
            return;
        }
        let outcomes = join_all(todo.iter().map(|id| self.refresh_one(id, force))).await;
        let stored = outcomes
            .iter()
            .filter(|o| **o == RefreshOutcome::Stored)
            .count();
        let failed = outcomes
            .iter()
            .filter(|o| **o == RefreshOutcome::Failed)
            .count();
        tracing::info!(
            "refresh venues={} took={}ms ok={stored} fail={failed}",
            todo.len(),
            started.elapsed().as_millis()
        );
        if stored > 0 {
            let mut last = self
                .last_success_at
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *last = Some(crate::adapters::now_ms());
            self.bus.publish(crate::bus::AppEvent::CatalogUpdated);
        }
    }

    /// Timestamp (unix ms) of the last refresh cycle in which at least one
    /// exchange actually succeeded, if any. Attempt time is never recorded.
    pub fn last_success_at(&self) -> Option<i64> {
        *self
            .last_success_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Search cached rows, restricted to the given exchanges:
    /// `base_symbol` matches exactly or by prefix, or the native `symbol`
    /// matches by substring (case insensitive). An empty query returns
    /// nothing — the Data page never dumps the whole market over IPC.
    pub fn search(&self, query: &str, exchange_ids: &[String]) -> Vec<MarketInfo> {
        let q = query.trim().to_lowercase();
        if q.is_empty() || exchange_ids.is_empty() {
            return Vec::new();
        }
        let data = self.data.read().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<MarketInfo> = data
            .values()
            .filter(|row| {
                if !exchange_ids.iter().any(|x| x == &row.exchange_id) {
                    return false;
                }
                let base = row.base_symbol.to_lowercase();
                base == q || base.starts_with(&q) || row.symbol.to_lowercase().contains(&q)
            })
            .cloned()
            .collect();
        out.sort_by(|a, b| {
            a.exchange_id
                .cmp(&b.exchange_id)
                .then_with(|| a.symbol.cmp(&b.symbol))
        });
        out
    }

    /// Fill the per-symbol gaps the bulk endpoints cannot provide, for
    /// exactly the rows a search returned (never for the whole market):
    /// true BBO for Hyperliquid/Entropy (`l2Book`), Lighter/RH
    /// (`orderBookOrders`) and Arcus (`/v1/bbo/{market}`). AsterDEX has
    /// no per-symbol fill: its BBO already comes from the bulk
    /// `bookTicker` call, and V3 has no openInterest endpoint, so its
    /// open interest stays `None`. The fills run concurrently but
    /// simultaneous requests, so a wide search cannot trigger 429s. A
    /// failed fill leaves the row as it was (bulk values stay; a missing
    /// BBO stays `None`, never a proxy) and logs one deduped warning per
    /// exchange.
    pub async fn enrich_rows(&self, rows: Vec<MarketInfo>) -> Vec<MarketInfo> {
        use futures_util::future::join_all;
        let futs: Vec<_> = rows
            .into_iter()
            .map(|row| async move {
                // One permit per row task, held for the whole fill: at most
                // ENRICH_CONCURRENCY per-market requests run at once.
                let _permit = self
                    .enrich_sem
                    .acquire()
                    .await
                    .expect("enrich semaphore is never closed");
                let mut row = row;
                if let Some(v) = crate::venues::all()
                    .iter()
                    .find(|v| v.id() == row.exchange_id.as_str())
                {
                    if let Err(e) = v.enrich(&self.http, &mut row).await {
                        self.log_refresh_error(
                            &format!("bbo-{}", row.exchange_id),
                            format!("{} 真实盘口补拉失败：{e}", row.exchange_id),
                        );
                    }
                }
                row
            })
            .collect();
        join_all(futs).await
    }

    /// The 5s-staleness fallback, for the search path. `stale_top5` is
    /// pure selection: the top rows (search order) whose WS book is
    /// stale. `merge_enriched` fills them once via REST and merges
    /// only the live fields back into the in-memory cache, so a
    /// re-search shows them. Returns true when any row actually
    /// changed — the page re-searches only then.
    pub fn stale_top5(
        rows: &[MarketInfo],
        is_stale: impl Fn(&str, &str) -> bool,
    ) -> Vec<MarketInfo> {
        rows.iter()
            .take(FALLBACK_TOP_N)
            .filter(|r| is_stale(&r.exchange_id, &r.symbol))
            .cloned()
            .collect()
    }

    pub async fn merge_enriched(&self, rows: Vec<MarketInfo>) -> bool {
        let enriched = self.enrich_rows(rows).await;
        let mut changed = false;
        let mut data = self.data.write().unwrap_or_else(|e| e.into_inner());
        for row in enriched {
            if let Some(cur) = data.get_mut(&(row.exchange_id.clone(), row.symbol.clone())) {
                if cur.bid_px != row.bid_px
                    || cur.ask_px != row.ask_px
                    || cur.mid_px != row.mid_px
                    || cur.open_interest_usd != row.open_interest_usd
                {
                    cur.bid_px = row.bid_px;
                    cur.ask_px = row.ask_px;
                    cur.mid_px = row.mid_px;
                    cur.open_interest_usd = row.open_interest_usd;
                    cur.updated_at = row.updated_at;
                    changed = true;
                }
            }
        }
        changed
    }

    /// Claim the fallback slot: at most one fallback task per 10s, so
    /// the 2s search tick cannot spawn one per tick. The slot is
    /// claimed at search time; the task itself waits 5s for WS frames
    /// before checking staleness.
    pub fn claim_fallback_slot(&self) -> bool {
        let mut slot = self.last_fallback.lock().unwrap_or_else(|e| e.into_inner());
        let due = slot
            .map(|t| t.elapsed() > Duration::from_secs(10))
            .unwrap_or(true);
        if due {
            *slot = Some(Instant::now());
        }
        due
    }

    /// Lighter market id for a cached row, for the realtime stream: the
    /// WebSocket `order_book` channel is addressed by numeric market id,
    /// not by symbol. None when the row is not cached (yet) or the
    /// exchange has no numeric ids.
    pub fn market_id(&self, exchange_id: &str, symbol: &str) -> Option<u64> {
        let data = self.data.read().unwrap_or_else(|e| e.into_inner());
        data.get(&(exchange_id.to_string(), symbol.to_string()))
            .and_then(|row| row.market_id)
    }

    pub fn len(&self) -> usize {
        self.data.read().unwrap_or_else(|e| e.into_inner()).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_matches_base_and_symbol_and_empty() {
        let catalog = MarketCatalog::new(EventBus::new(16));
        let all: Vec<String> = crate::venues::all()
            .iter()
            .map(|v| v.id().to_string())
            .collect();
        let all = &all;
        catalog.store(
            "hyperliquid",
            vec![MarketInfo {
                exchange_id: "hyperliquid".into(),
                symbol: "BTC".into(),
                base_symbol: "BTC".into(),
                market_id: None,
                settlement_currency: "USDC".into(),
                max_leverage: Some(40.0),
                tick_size: Some(1.0),
                step_size: Some(0.00001),
                min_notional: Some(10.0),
                bid_px: None,
                ask_px: None,
                mid_px: None,
                mark_px: Some(58033.0),
                index_px: None,
                funding_rate_raw: None,
                funding_interval_hours: Some(1.0),
                funding_1h: None,
                funding_apr: None,
                next_funding_time: None,
                volume_24h_usd: None,
                open_interest_usd: None,
                updated_at: 0,
            }],
        );
        catalog.store(
            "entropy",
            vec![MarketInfo {
                exchange_id: "entropy".into(),
                symbol: "io:NVDA".into(),
                base_symbol: "NVDA".into(),
                market_id: None,
                settlement_currency: "USDC".into(),
                max_leverage: Some(10.0),
                tick_size: Some(0.01),
                step_size: Some(0.001),
                min_notional: Some(10.0),
                bid_px: None,
                ask_px: None,
                mid_px: None,
                mark_px: Some(143.0),
                index_px: None,
                funding_rate_raw: None,
                funding_interval_hours: Some(1.0),
                funding_1h: None,
                funding_apr: None,
                next_funding_time: None,
                volume_24h_usd: None,
                open_interest_usd: None,
                updated_at: 0,
            }],
        );
        assert_eq!(catalog.search("", all), Vec::<MarketInfo>::new());
        assert_eq!(catalog.search("   ", all), Vec::<MarketInfo>::new());
        let btc = catalog.search("btc", all);
        assert_eq!(btc.len(), 1);
        assert_eq!(btc[0].exchange_id, "hyperliquid");
        // Prefix on base_symbol also matches.
        assert_eq!(catalog.search("BT", all).len(), 1);
        assert_eq!(catalog.search("NV", all).len(), 1);
        // Exchange filter: BTC lives only under hyperliquid, so searching
        // it with only entropy checked returns nothing.
        let only_entropy = vec!["entropy".to_string()];
        assert!(catalog.search("btc", &only_entropy).is_empty());
        assert_eq!(catalog.search("NVDA", &only_entropy).len(), 1);
        // Matches base_symbol even though the native symbol has the io: prefix.
        let nvda = catalog.search("NVDA", all);
        assert_eq!(nvda.len(), 1);
        assert_eq!(nvda[0].symbol, "io:NVDA");
        // Substring on the native symbol also matches.
        assert_eq!(catalog.search("io:", all).len(), 1);

        assert!(catalog.search("DOGE", all).is_empty());
    }

    /// Same exchange, same lock instance (so two refreshes serialize);
    /// different exchanges get different locks (so they run concurrently).
    /// A second acquirer for a held lock must wait, while another
    /// exchange's lock proceeds immediately.
    #[tokio::test]
    async fn refresh_locks_serialize_per_exchange() {
        let catalog = MarketCatalog::new(EventBus::new(16));
        let lock_a1 = catalog.refresh_lock("hyperliquid");
        let lock_a2 = catalog.refresh_lock("hyperliquid");
        let lock_b = catalog.refresh_lock("arcus");
        assert!(Arc::ptr_eq(&lock_a1, &lock_a2));
        assert!(!Arc::ptr_eq(&lock_a1, &lock_b));
        let guard_a = lock_a1.lock().await;
        // Another exchange proceeds while hyperliquid is held.
        assert!(lock_b.try_lock().is_ok());
        // The same exchange must wait.
        assert!(lock_a2.try_lock().is_err());
        drop(guard_a);
        assert!(lock_a2.try_lock().is_ok());
    }

    /// The enrichment semaphore caps concurrent per-market requests at
    /// ENRICH_CONCURRENCY (10): the 11th acquire blocks while all permits
    /// are held, and proceeds once one is released.
    #[test]
    fn enrich_semaphore_caps_concurrency() {
        let catalog = MarketCatalog::new(EventBus::new(16));
        let mut permits = Vec::new();
        for _ in 0..ENRICH_CONCURRENCY {
            permits.push(
                catalog
                    .enrich_sem
                    .try_acquire()
                    .expect("permit within the cap"),
            );
        }
        assert!(catalog.enrich_sem.try_acquire().is_err());
        drop(permits.pop());
        assert!(catalog.enrich_sem.try_acquire().is_ok());
    }

    /// Cold-start detection (incident 2026-11-26: ordering without
    /// visiting the Data page failed with "不在品种目录中" because the
    /// catalog was empty): `has_rows_for` reports a cold exchange, and
    /// `ensure_exchange_loaded` short-circuits without any fetch once
    /// rows are present. This test uses injected rows, so it never
    /// touches the network; the live-fetch half is covered by the
    /// execution test `place_order_loads_cold_catalog_for_its_exchange`.
    #[tokio::test]
    async fn ensure_exchange_loaded_skips_loaded_exchange() {
        let catalog = MarketCatalog::new(EventBus::new(16));
        assert!(!catalog.has_rows_for("hyperliquid"));
        catalog.store(
            "hyperliquid",
            vec![MarketInfo {
                exchange_id: "hyperliquid".into(),
                symbol: "BTC".into(),
                base_symbol: "BTC".into(),
                market_id: None,
                settlement_currency: "USDC".into(),
                max_leverage: Some(40.0),
                tick_size: Some(1.0),
                step_size: Some(0.00001),
                min_notional: Some(10.0),
                bid_px: None,
                ask_px: None,
                mid_px: None,
                mark_px: Some(58033.0),
                index_px: None,
                funding_rate_raw: None,
                funding_interval_hours: Some(1.0),
                funding_1h: None,
                funding_apr: None,
                next_funding_time: None,
                volume_24h_usd: None,
                open_interest_usd: None,
                updated_at: 0,
            }],
        );
        assert!(catalog.has_rows_for("hyperliquid"));
        assert!(catalog.ensure_exchange_loaded("hyperliquid").await);
        assert!(catalog.last_refresh_error("hyperliquid").is_none());
        // No other exchange was touched.
        assert!(!catalog.has_rows_for("entropy"));
        assert!(catalog.last_refresh_error("entropy").is_none());
    }

    /// PR-0.3: the refresh gate is TTL + backoff, decided without
    /// network. Fresh (inside 30s) skips; a failed exchange backs off
    /// 60s; `force` (manual refresh) overrides both; a later success
    /// clears the backoff.
    #[test]
    fn refresh_gate_ttl_and_backoff() {
        let catalog = MarketCatalog::new(EventBus::new(16));
        // Nothing cached, no error: due.
        assert!(catalog.should_refresh("hyperliquid", false));
        // Just fetched: inside TTL, skip — unless forced.
        catalog.mark_fetched("hyperliquid");
        assert!(!catalog.should_refresh("hyperliquid", false));
        assert!(catalog.should_refresh("hyperliquid", true));
        // A failed exchange backs off (no error text needed here).
        assert!(catalog.should_refresh("entropy", false));
        catalog.apply("entropy", Err("boom".to_string()));
        assert!(catalog.in_backoff("entropy"));
        assert!(!catalog.should_refresh("entropy", false));
        assert!(catalog.should_refresh("entropy", true));
        // A later success clears the backoff.
        catalog.apply("entropy", Ok(vec![]));
        assert!(!catalog.in_backoff("entropy"));
    }

    /// PR-0.3: an unknown symbol misses locally but the exchange is
    /// due exactly one forced refresh; once that fails, the 60s
    /// backoff stops further retries (no retry on every keystroke).
    #[test]
    fn unknown_symbol_miss_then_backoff() {
        let catalog = MarketCatalog::new(EventBus::new(16));
        catalog.store(
            "hyperliquid",
            vec![MarketInfo {
                exchange_id: "hyperliquid".into(),
                symbol: "BTC".into(),
                base_symbol: "BTC".into(),
                market_id: None,
                settlement_currency: "USDC".into(),
                max_leverage: Some(40.0),
                tick_size: Some(1.0),
                step_size: Some(0.00001),
                min_notional: Some(10.0),
                bid_px: None,
                ask_px: None,
                mid_px: None,
                mark_px: Some(58033.0),
                index_px: None,
                funding_rate_raw: None,
                funding_interval_hours: Some(1.0),
                funding_1h: None,
                funding_apr: None,
                next_funding_time: None,
                volume_24h_usd: None,
                open_interest_usd: None,
                updated_at: 0,
            }],
        );
        let all: Vec<String> = crate::venues::all()
            .iter()
            .map(|v| v.id().to_string())
            .collect();
        assert!(catalog.search("ZZZZ", &all).is_empty());
        // The miss leaves the exchange due exactly one refresh...
        assert!(catalog.should_refresh("hyperliquid", false));
        // ...and a failed refresh backs it off instead of retrying.
        catalog.apply("hyperliquid", Err("boom".to_string()));
        assert!(!catalog.should_refresh("hyperliquid", false));
    }

    /// PR-0.3: cold start loads instruments from SQLite with no
    /// network — statics survive, quotes start empty (prices show
    /// "—" until a refresh or WS frame). Timings are asserted
    /// generously (temp dir, tiny DB) and printed for the record.
    #[test]
    fn cold_start_loads_instruments_from_sqlite() {
        use crate::data_engine::DataEngine;
        use crate::portable::PortablePaths;
        let dir = std::env::temp_dir().join(format!("desss-pr03-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let paths = PortablePaths {
            root: dir.clone(),
            data_dir: dir.clone(),
            db_path: dir.join("desss.db"),
            log_dir: dir.join("logs"),
            vault_key_path: dir.join("vault.key"),
            vault_enc_path: dir.join("vault.enc"),
        };
        let db = Arc::new(DataEngine::open(paths).unwrap());
        let row = MarketInfo {
            exchange_id: "hyperliquid".into(),
            symbol: "BTC".into(),
            base_symbol: "BTC".into(),
            market_id: None,
            settlement_currency: "USDC".into(),
            max_leverage: Some(40.0),
            tick_size: Some(1.0),
            step_size: Some(0.00001),
            min_notional: Some(10.0),
            bid_px: None,
            ask_px: None,
            mid_px: None,
            mark_px: Some(58033.0),
            index_px: None,
            funding_rate_raw: None,
            funding_interval_hours: Some(1.0),
            funding_1h: Some(0.0001),
            funding_apr: Some(87.6),
            next_funding_time: None,
            volume_24h_usd: Some(1_000_000.0),
            open_interest_usd: Some(2_000_000.0),
            updated_at: 1_700_000_000_000,
        };
        // Seed via one catalog, then cold-load via a fresh one.
        {
            let catalog = MarketCatalog::with_db(EventBus::new(16), db.clone());
            catalog.store("hyperliquid", vec![row.clone()]);
            assert!(catalog.has_rows_for("hyperliquid"));
        }
        let t0 = std::time::Instant::now();
        let cold = MarketCatalog::with_db(EventBus::new(16), db.clone());
        let load_ms = t0.elapsed().as_millis();
        let all: Vec<String> = crate::venues::all()
            .iter()
            .map(|v| v.id().to_string())
            .collect();
        let t0 = std::time::Instant::now();
        let hits = cold.search("BTC", &all);
        let first_ms = t0.elapsed().as_millis();
        assert_eq!(hits.len(), 1);
        // Statics survived the round trip; quotes start empty.
        assert_eq!(hits[0].tick_size, Some(1.0));
        assert_eq!(hits[0].max_leverage, Some(40.0));
        assert_eq!(hits[0].mark_px, None);
        let t0 = std::time::Instant::now();
        let hits2 = cold.search("BTC", &all);
        let hot_ms = t0.elapsed().as_millis();
        assert_eq!(hits2.len(), 1);
        eprintln!("cold-start: load={load_ms}ms first_search={first_ms}ms hot_search={hot_ms}ms");
        assert!(load_ms + first_ms < 200, "cold first search under 200ms");
        assert!(hot_ms < 100, "hot search under 100ms");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
