//! Bulk market catalog (phase 4): contract specs + live prices + funding rates
//! for all six exchanges, standardized into [`MarketInfo`] and served to the
//! Data page's symbol search.
//!
//! On-demand only: there is no background refresh loop. A search or manual
//! refresh fetches (concurrently) the checked exchanges whose cache is
//! older than [`CACHE_TTL`]; otherwise the cached rows are served. With no
//! search and no refresh, the catalog makes zero requests.
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
//!   24h volume comes from `ticker/24hr` (`quoteVolume`); open interest
//!   is filled per search hit via `openInterest?symbol=..` (base units x
//!   mark price). `max_leverage` has **no public source** (exchangeInfo
//!   carries no leverage field; `maintMarginPercent` 2.5% /
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
//!   AsterDEX `ticker/24hr quoteVolume` + per-symbol `openInterest * markPx`.
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
//! - Two cache layers: the static catalog (specs, leverage, tick sizes)
//!   keeps the 30s bulk cache; the live fields of search hits (mark
//!   price, funding) refresh on a short 2s cycle via `refresh_live_rows`
//!   (per-exchange live snapshots), so a watched search ticks without
//!   refetching the whole catalog.
//! - `last_success_at` records the last cycle in which at least one
//!   exchange actually succeeded — attempt time is never recorded, so a
//!   fully failed cycle leaves the "缓存更新于" timestamp untouched.
//! - This module is Tauri-free, so it runs in the scratch test crate.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::arcus::ARCUS_REST_URL;
use super::asterdex::ASTER_REST_URL;
use super::hyperliquid::HL_INFO_URL;
use super::lighter::LighterNetwork;
use crate::bus::EventBus;
use crate::net::shared_http_client;

/// Cached rows are fresh for 30s: a search inside that window serves the
/// cache with no request; older rows are refetched on demand.
pub const CACHE_TTL: Duration = Duration::from_secs(30);
/// AsterDEX specs change slowly (890 KB payload) — refetch at most hourly.
const ASTER_SPECS_TTL: Duration = Duration::from_secs(3600);
/// Live fields of search hits (mark price, funding) refresh at most once
/// per exchange per this interval — the watched-search tick cycle.
const LIVE_TTL: Duration = Duration::from_secs(2);
/// Max concurrent per-market enrichment requests (BBO / OI fills): wide
/// searches fan out over many symbols, and the venues rate-limit (429)
/// above roughly a dozen parallel calls.
const ENRICH_CONCURRENCY: usize = 10;

/// Exchange ids the catalog knows, in display order.
pub const KNOWN_EXCHANGES: [&str; 6] = [
    "hyperliquid",
    "entropy",
    "lighter",
    "lighter_robinhood",
    "asterdex",
    "arcus",
];

/// One standardized market row, shared by all six exchanges. `None` means
/// the exchange has no public source for that field (e.g. AsterDEX max
/// leverage — there is no public endpoint for it), or the value could not
/// be confirmed (e.g. an AsterDEX funding interval missing from
/// `fundingInfo` leaves the interval, the 1h rate and the APR all `None`);
/// the frontend renders it as "—". Bid/ask come only from a real order
/// book (per-hit fill); a bulk proxy is never written here. `market_id`
/// is internal only (Lighter BBO fill) and never serialized.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MarketInfo {
    pub exchange_id: String,
    pub symbol: String,
    pub base_symbol: String,
    #[serde(default, skip_serializing)]
    pub market_id: Option<u64>,
    pub settlement_currency: String,
    pub max_leverage: Option<f64>,
    pub tick_size: Option<f64>,
    pub step_size: Option<f64>,
    pub min_notional: Option<f64>,
    pub bid_px: Option<f64>,
    pub ask_px: Option<f64>,
    pub mid_px: Option<f64>,
    pub mark_px: Option<f64>,
    pub index_px: Option<f64>,
    pub funding_rate_raw: Option<f64>,
    /// Funding settlement period in hours: 1 on HL/Entropy/Lighter/Arcus,
    /// per-symbol 1/2/4/8 on AsterDEX (from fundingInfo). `None` means the
    /// period could not be confirmed (AsterDEX symbol missing from
    /// fundingInfo) — then `funding_1h` and `funding_apr` are `None` too.
    /// Note the two Lighter numbers differ by design: the published `rate`
    /// is an 8h-equivalent quote, so funding_1h = raw / 8, while the venue
    /// settles hourly (interval 1h). Never derive funding_1h from this
    /// field alone — each parser applies its venue's quote period.
    pub funding_interval_hours: Option<f64>,
    /// Funding rate normalized to one hour (raw / interval).
    pub funding_1h: Option<f64>,
    /// Annualized funding rate in percent, simple multiple (1h * 24 * 365 * 100).
    pub funding_apr: Option<f64>,
    /// Next funding settlement, unix ms (None when the exchange gives none).
    pub next_funding_time: Option<i64>,
    /// 24h volume in USD (None when the exchange doesn't publish it).
    pub volume_24h_usd: Option<f64>,
    /// Open interest in USD (None when unavailable).
    pub open_interest_usd: Option<f64>,
    /// When this row was built, unix ms.
    pub updated_at: i64,
}

/// Normalize an exchange-native symbol to a cross-exchange base name:
/// strip the `io:` prefix (Entropy), split off a `/QUOTE` or `-QUOTE`
/// suffix (Lighter `NVDA/USDG`, Arcus `BTC-USD`), strip a leading `rh`
/// (Lighter `rhSPY/USDC`), strip a trailing quote suffix (AsterDEX
/// `BTCUSDT`). `1000BONK`-style prefixes and FX pairs (`USDCAD`) are kept
/// as-is — they are distinct listings.
pub fn normalize_base_symbol(symbol: &str) -> String {
    let mut s = symbol.trim().to_uppercase();
    if let Some(rest) = s.strip_prefix("IO:") {
        s = rest.to_string();
    }
    // "NVDA/USDG" -> "NVDA"; "rhSPY/USDC" -> "SPY".
    let mut base = if let Some((head, _)) = s.split_once('/') {
        let mut head = head.to_string();
        if head.len() > 2 && head.starts_with("RH") {
            head = head[2..].to_string();
        }
        head
    } else {
        s.clone()
    };
    if let Some((head, _)) = base.split_once('-') {
        base = head.to_string();
    }
    for suffix in ["USDT", "USDC", "USDG", "USD"] {
        if base.len() > suffix.len() && base.ends_with(suffix) {
            base.truncate(base.len() - suffix.len());
            break;
        }
    }
    base
}

/// Hourly funding rate from a raw periodic rate.
pub fn funding_1h_from(raw: f64, interval_hours: f64) -> f64 {
    raw / interval_hours
}

/// Annualized funding rate in percent (simple multiple, no compounding).
pub fn funding_apr_pct(funding_1h: f64) -> f64 {
    funding_1h * 24.0 * 365.0 * 100.0
}

/// Next whole-hour UTC boundary after `now_ms` (unix ms), used by every
/// 1h-funding venue (their APIs either give no timestamp or an unreliable
/// one — Hyperliquid's `predictedFundings.nextFundingTime` was observed
/// returning a past hour).
pub fn next_hour_utc_ms(now_ms: i64) -> i64 {
    (now_ms / 3_600_000 + 1) * 3_600_000
}

/// Hyperliquid tick size: prices must satisfy <= 5 significant figures AND
/// <= (6 - szDecimals) decimals, so the effective tick is the larger of the
/// two constraints. Verified live: BTC ~$58k -> $1.00, ETH ~$1641 -> $0.10.
pub fn hyperliquid_tick_size(mark_px: f64, sz_decimals: i64) -> f64 {
    let decimals_tick = 10f64.powi(-(6 - sz_decimals) as i32);
    if mark_px <= 0.0 {
        return decimals_tick;
    }
    let sig_tick = 10f64.powi(mark_px.log10().floor() as i32 - 4);
    sig_tick.max(decimals_tick)
}

/// A JSON number-or-numeric-string as f64; missing / non-numeric reads as
/// None (unlike `f64_from`, which defaults to 0.0 — wrong for optional
/// market fields, where absence must stay absent).
fn opt_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn opt_str<'a>(v: &'a serde_json::Value) -> Option<&'a str> {
    v.as_str()
}

fn str_field<'a>(v: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(opt_str)
}

fn num_field(v: &serde_json::Value, key: &str) -> Option<f64> {
    v.get(key).and_then(opt_f64)
}

/// AsterDEX contract specs (from exchangeInfo, refreshed at most hourly).
#[derive(Clone, Debug)]
struct AsterSpec {
    base_symbol: String,
    settlement_currency: String,
    tick_size: Option<f64>,
    step_size: Option<f64>,
    min_notional: Option<f64>,
}

fn aster_specs_from(v: &serde_json::Value) -> HashMap<String, AsterSpec> {
    let mut out = HashMap::new();
    let Some(list) = v.get("symbols").and_then(|s| s.as_array()) else {
        return out;
    };
    for s in list {
        if str_field(s, "status") != Some("TRADING") {
            continue;
        }
        if str_field(s, "contractType") != Some("PERPETUAL") {
            continue;
        }
        let Some(symbol) = str_field(s, "symbol") else {
            continue;
        };
        let mut tick = None;
        let mut step = None;
        let mut min_notional = None;
        if let Some(filters) = s.get("filters").and_then(|f| f.as_array()) {
            for f in filters {
                match str_field(f, "filterType") {
                    Some("PRICE_FILTER") => tick = num_field(f, "tickSize"),
                    Some("LOT_SIZE") => step = num_field(f, "stepSize"),
                    Some("MIN_NOTIONAL") => min_notional = num_field(f, "notional"),
                    _ => {}
                }
            }
        }
        out.insert(
            symbol.to_string(),
            AsterSpec {
                base_symbol: str_field(s, "baseAsset")
                    .map(normalize_base_symbol)
                    .unwrap_or_else(|| normalize_base_symbol(symbol)),
                settlement_currency: str_field(s, "marginAsset").unwrap_or("USDT").to_string(),
                tick_size: tick,
                step_size: step,
                min_notional,
            },
        );
    }
    out
}

/// Parse one Hyperliquid/Entropy `metaAndAssetCtxs` response (`[meta, ctxs]`,
/// aligned by index). `exchange_id` is "hyperliquid" or "entropy".
fn parse_hyperliquid(
    v: &serde_json::Value,
    exchange_id: &str,
    now_ms: i64,
) -> Result<Vec<MarketInfo>, String> {
    let (meta, ctxs) = match v.as_array() {
        Some(a) if a.len() == 2 => (&a[0], &a[1]),
        _ => return Err("metaAndAssetCtxs 返回结构异常（应为 [meta, assetCtxs]）".into()),
    };
    let Some(universe) = meta.get("universe").and_then(|u| u.as_array()) else {
        return Err("metaAndAssetCtxs 缺少 universe".into());
    };
    let Some(ctx_list) = ctxs.as_array() else {
        return Err("metaAndAssetCtxs 缺少 assetCtxs".into());
    };
    let next_funding = next_hour_utc_ms(now_ms);
    let mut out = Vec::new();
    for (i, u) in universe.iter().enumerate() {
        if u.get("isDelisted")
            .and_then(|d| d.as_bool())
            .unwrap_or(false)
        {
            continue;
        }
        let Some(symbol) = str_field(u, "name") else {
            continue;
        };
        let Some(ctx) = ctx_list.get(i) else {
            continue;
        };
        let sz_decimals = u.get("szDecimals").and_then(|n| n.as_i64()).unwrap_or(0);
        let mark_px = num_field(ctx, "markPx");
        let funding = num_field(ctx, "funding");
        // No bid/ask in the bulk response: `impactPxs` is an
        // execution-price proxy, never top-of-book, and must not be shown
        // as the BBO. Bid/ask stay None until the per-hit `l2Book` fill;
        // a failed fill leaves them None as well.
        let tick = mark_px.map(|px| hyperliquid_tick_size(px, sz_decimals));
        // Open interest is quoted in base units; convert with the mark price.
        let open_interest_usd = match (num_field(ctx, "openInterest"), mark_px) {
            (Some(oi), Some(px)) => Some(oi * px),
            _ => None,
        };
        out.push(MarketInfo {
            exchange_id: exchange_id.to_string(),
            symbol: symbol.to_string(),
            base_symbol: normalize_base_symbol(symbol),
            market_id: None,
            settlement_currency: "USDC".to_string(),
            max_leverage: num_field(u, "maxLeverage"),
            tick_size: tick,
            step_size: Some(10f64.powi(-sz_decimals as i32)),
            min_notional: Some(10.0),
            bid_px: None,
            ask_px: None,
            mid_px: num_field(ctx, "midPx"),
            mark_px,
            index_px: num_field(ctx, "oraclePx"),
            funding_rate_raw: funding,
            funding_interval_hours: Some(1.0),
            funding_1h: funding,
            funding_apr: funding.map(funding_apr_pct),
            next_funding_time: Some(next_funding),
            volume_24h_usd: num_field(ctx, "dayNtlVlm"),
            open_interest_usd,
            updated_at: now_ms,
        });
    }
    Ok(out)
}

/// Parse Lighter / Lighter RH `orderBookDetails` + `funding-rates` responses.
/// Funding rows are keyed by market_id and filtered to `exchange == "lighter"`.
fn parse_lighter(
    details: &serde_json::Value,
    funding: &serde_json::Value,
    exchange_id: &str,
    settlement_currency: &str,
    now_ms: i64,
) -> Result<Vec<MarketInfo>, String> {
    let Some(rows) = details.get("order_book_details").and_then(|r| r.as_array()) else {
        return Err("orderBookDetails 缺少 order_book_details".into());
    };
    let mut funding_by_id: HashMap<u64, f64> = HashMap::new();
    if let Some(frows) = funding.get("funding_rates").and_then(|r| r.as_array()) {
        for f in frows {
            if str_field(f, "exchange") != Some("lighter") {
                continue;
            }
            if let (Some(mid), Some(rate)) = (
                f.get("market_id").and_then(|n| n.as_u64()),
                num_field(f, "rate"),
            ) {
                funding_by_id.insert(mid, rate);
            }
        }
    }
    let next_funding = next_hour_utc_ms(now_ms);
    let mut out = Vec::new();
    for r in rows {
        if let Some(t) = str_field(r, "market_type") {
            if t != "perp" {
                continue;
            }
        }
        if let Some(s) = str_field(r, "status") {
            if s != "active" {
                continue;
            }
        }
        let Some(symbol) = str_field(r, "symbol") else {
            continue;
        };
        let Some(mid) = r.get("market_id").and_then(|n| n.as_u64()) else {
            continue;
        };
        let price_decimals = r
            .get("price_decimals")
            .and_then(|n| n.as_i64())
            .unwrap_or(0);
        let size_decimals = r.get("size_decimals").and_then(|n| n.as_i64()).unwrap_or(0);
        // Max leverage = 10000 / min_initial_margin_fraction (validated
        // live: BTC 200 -> 50x, SOL 400 -> 25x).
        let max_leverage = r
            .get("min_initial_margin_fraction")
            .and_then(opt_f64)
            .filter(|f| *f > 0.0)
            .map(|f| 10000.0 / f);
        let mark_px = num_field(r, "mark_price");
        let funding_raw = funding_by_id.get(&mid).copied();
        // The funding-rates `rate` is an 8h-equivalent quote; the venue
        // settles hourly, so the hourly rate is rate / 8. The settlement
        // period stays 1h — the 8 belongs to the quote, not the interval.
        let funding_1h = funding_raw.map(|x| funding_1h_from(x, 8.0));
        out.push(MarketInfo {
            exchange_id: exchange_id.to_string(),
            symbol: symbol.to_string(),
            base_symbol: normalize_base_symbol(symbol),
            market_id: Some(mid),
            settlement_currency: settlement_currency.to_string(),
            max_leverage,
            tick_size: Some(10f64.powi(-price_decimals as i32)),
            step_size: Some(10f64.powi(-size_decimals as i32)),
            min_notional: num_field(r, "min_quote_amount"),
            bid_px: None,
            ask_px: None,
            // No bulk bid/ask in REST; last trade price is the closest
            mid_px: num_field(r, "last_trade_price").or(mark_px),
            mark_px,
            index_px: num_field(r, "index_price"),
            funding_rate_raw: funding_raw,
            funding_interval_hours: Some(1.0),
            funding_1h,
            funding_apr: funding_1h.map(funding_apr_pct),
            next_funding_time: Some(next_funding),
            volume_24h_usd: num_field(r, "daily_quote_token_volume"),
            open_interest_usd: num_field(r, "open_interest_usd"),
            updated_at: now_ms,
        });
    }
    Ok(out)
}

/// Parse AsterDEX `premiumIndex` + `bookTicker`, joined on the specs map
/// (TRADING PERPETUAL symbols only; premiumIndex carries ~156 stale entries
/// that are not in exchangeInfo and are ignored). `intervals` maps symbol
/// to its funding settlement period in hours (from `fundingInfo`); a symbol
/// missing from it has an **unknown** interval — `funding_interval_hours`,
/// `funding_1h` and `funding_apr` all stay `None`, never a defaulted 8h.
/// `volumes` maps symbol to 24h volume in USD (from `ticker/24hr`
/// quoteVolume). `max_leverage` is always `None`: no public source exists.
fn parse_asterdex(
    specs: &HashMap<String, AsterSpec>,
    premium: &serde_json::Value,
    book: &serde_json::Value,
    intervals: &HashMap<String, f64>,
    volumes: &HashMap<String, f64>,
    now_ms: i64,
) -> Result<Vec<MarketInfo>, String> {
    let Some(premium_list) = premium.as_array() else {
        return Err("premiumIndex 返回结构异常（应为数组）".into());
    };
    let mut book_by_symbol: HashMap<&str, &serde_json::Value> = HashMap::new();
    if let Some(book_list) = book.as_array() {
        for b in book_list {
            if let Some(s) = str_field(b, "symbol") {
                book_by_symbol.insert(s, b);
            }
        }
    }
    let mut out = Vec::new();
    for p in premium_list {
        let Some(symbol) = str_field(p, "symbol") else {
            continue;
        };
        let Some(spec) = specs.get(symbol) else {
            continue; // stale/delisted entry not in exchangeInfo
        };
        let funding_raw = num_field(p, "lastFundingRate");
        // Funding interval is per-symbol (fundingInfo: 1h/2h/4h/8h).
        // Unknown interval -> no 1h rate and no APR: a fabricated number
        // is worse than an honest "—".
        let interval = intervals.get(symbol).copied().filter(|h| *h > 0.0);
        let funding_1h = match (funding_raw, interval) {
            (Some(raw), Some(h)) => Some(funding_1h_from(raw, h)),
            _ => None,
        };
        let next_funding = p
            .get("nextFundingTime")
            .and_then(|n| n.as_i64())
            .filter(|t| *t > 0);
        let (bid_px, ask_px) = match book_by_symbol.get(symbol) {
            Some(b) => (num_field(b, "bidPrice"), num_field(b, "askPrice")),
            None => (None, None),
        };
        let mark_px = num_field(p, "markPrice");
        out.push(MarketInfo {
            exchange_id: "asterdex".to_string(),
            symbol: symbol.to_string(),
            base_symbol: spec.base_symbol.clone(),
            market_id: None,
            settlement_currency: spec.settlement_currency.clone(),
            // No public source for max leverage (exchangeInfo has no
            // leverage field; maint/required margin percents are not max
            // leverage). The catalog never signs requests with account
            // credentials, so this stays null.
            max_leverage: None,
            tick_size: spec.tick_size,
            step_size: spec.step_size,
            min_notional: spec.min_notional,
            bid_px,
            ask_px,
            mid_px: match (bid_px, ask_px) {
                (Some(b), Some(a)) => Some((b + a) / 2.0),
                _ => None,
            },
            mark_px,
            index_px: num_field(p, "indexPrice"),
            funding_rate_raw: funding_raw,
            funding_interval_hours: interval,
            funding_1h,
            funding_apr: funding_1h.map(funding_apr_pct),
            next_funding_time: next_funding,
            // 24h volume from ticker/24hr (quoteVolume, USDT); open
            // interest is filled per search hit (openInterest is per-symbol).
            volume_24h_usd: volumes.get(symbol).copied(),
            open_interest_usd: None,
            updated_at: now_ms,
        });
    }
    Ok(out)
}

/// Parse Arcus `GET /v1/markets` (covers specs, prices and funding in one
/// call). `nextFundingAt` is in **seconds**, not ms. Max leverage is
/// 1 / initialMarginFraction; RWA markets use the off-hours fraction when
/// outside regular trading hours.
fn parse_arcus(v: &serde_json::Value, now_ms: i64) -> Result<Vec<MarketInfo>, String> {
    let Some(list) = v.get("markets").and_then(|m| m.as_array()) else {
        return Err("/v1/markets 缺少 markets 数组".into());
    };
    let mut out = Vec::new();
    for m in list {
        if str_field(m, "status") != Some("ONLINE") {
            continue;
        }
        if let Some(t) = str_field(m, "type") {
            if t != "PERPETUAL" {
                continue;
            }
        }
        let Some(symbol) = str_field(m, "marketDisplayName") else {
            continue;
        };
        let outside_rth = m
            .get("isOutsideRth")
            .and_then(|b| b.as_bool())
            .unwrap_or(false);
        let imf = if outside_rth {
            num_field(m, "offHoursInitialMarginFraction")
                .or_else(|| num_field(m, "initialMarginFraction"))
        } else {
            num_field(m, "initialMarginFraction")
        };
        let max_leverage = imf.filter(|f| *f > 0.0).map(|f| 1.0 / f);
        let funding_raw = num_field(m, "fundingRate");
        let next_funding = m
            .get("nextFundingAt")
            .and_then(|n| n.as_i64())
            .map(|s| s * 1000);
        let mark_px = num_field(m, "markPrice");
        // Open interest is quoted in base units; convert with the mark price.
        let open_interest_usd = match (num_field(m, "openInterest"), mark_px) {
            (Some(oi), Some(px)) => Some(oi * px),
            _ => None,
        };
        out.push(MarketInfo {
            exchange_id: "arcus".to_string(),
            symbol: symbol.to_string(),
            base_symbol: str_field(m, "baseAsset")
                .map(normalize_base_symbol)
                .unwrap_or_else(|| normalize_base_symbol(symbol)),
            market_id: None,
            settlement_currency: "USDG".to_string(),
            max_leverage,
            tick_size: num_field(m, "tickSize"),
            step_size: num_field(m, "stepSize"),
            min_notional: num_field(m, "minOrderNotional"),
            bid_px: None,
            ask_px: None,
            mid_px: None,
            mark_px: num_field(m, "markPrice"),
            index_px: num_field(m, "oraclePrice"),
            funding_rate_raw: funding_raw,
            funding_interval_hours: Some(1.0),
            funding_1h: funding_raw,
            funding_apr: funding_raw.map(funding_apr_pct),
            next_funding_time: next_funding,
            volume_24h_usd: num_field(m, "volume24hNotional"),
            open_interest_usd,
            updated_at: now_ms,
        });
    }
    Ok(out)
}

async fn get_json(http: &reqwest::Client, url: &str) -> Result<serde_json::Value, String> {
    // One retry on a stale pooled connection (SendRequest /
    // UnexpectedEof); error text is redacted (no URL query params).
    crate::net::read_json(|| http.get(url)).await
}

async fn fetch_hyperliquid(
    http: &reqwest::Client,
    dex: Option<&str>,
) -> Result<Vec<MarketInfo>, String> {
    let mut body = serde_json::json!({ "type": "metaAndAssetCtxs" });
    let exchange_id = match dex {
        Some("io") => {
            body["dex"] = serde_json::json!("io");
            "entropy"
        }
        _ => "hyperliquid",
    };
    let v = crate::net::read_json(|| http.post(HL_INFO_URL).json(&body)).await?;
    parse_hyperliquid(&v, exchange_id, crate::adapters::now_ms())
}

async fn fetch_lighter(
    http: &reqwest::Client,
    network: LighterNetwork,
) -> Result<Vec<MarketInfo>, String> {
    let base = network.rest_base();
    let (exchange_id, settlement) = match network {
        LighterNetwork::Mainnet => ("lighter", "USDC"),
        LighterNetwork::Robinhood => ("lighter_robinhood", "USDG"),
    };
    let details_url = format!("{base}/api/v1/orderBookDetails");
    let funding_url = format!("{base}/api/v1/funding-rates");
    let (details, funding) =
        tokio::join!(get_json(http, &details_url), get_json(http, &funding_url),);
    let details = details?;
    let funding = funding?;
    parse_lighter(
        &details,
        &funding,
        exchange_id,
        settlement,
        crate::adapters::now_ms(),
    )
}

/// True top-of-book from a Hyperliquid `l2Book` response: levels[0] are
/// bids (best first), levels[1] asks (best first). Thin markets may
/// return empty sides — those stay None, never 0.
fn parse_l2book(v: &serde_json::Value) -> (Option<f64>, Option<f64>) {
    let Some(levels) = v.get("levels").and_then(|l| l.as_array()) else {
        return (None, None);
    };
    let first_px = |side: Option<&serde_json::Value>| -> Option<f64> {
        side.and_then(|s| s.as_array())
            .and_then(|a| a.first())
            .and_then(|row| num_field(row, "px"))
    };
    (first_px(levels.first()), first_px(levels.get(1)))
}

/// Best bid/ask from a Lighter `orderBookOrders?limit=2` response. The
/// venue sorts asks ascending and bids descending; with two orders per
/// side the min/max is taken anyway so a sort surprise cannot flip the top.
fn parse_lighter_book_orders(v: &serde_json::Value) -> (Option<f64>, Option<f64>) {
    let best = |key: &str, want_min: bool| -> Option<f64> {
        let list = v.get(key)?.as_array()?;
        list.iter()
            .filter_map(|o| num_field(o, "price"))
            .fold(None, |acc: Option<f64>, x| {
                Some(match acc {
                    None => x,
                    Some(a) => {
                        if want_min {
                            a.min(x)
                        } else {
                            a.max(x)
                        }
                    }
                })
            })
    };
    (best("bids", false), best("asks", true))
}

/// Best bid/ask from an Arcus `GET /v1/bbo/{market}` response.
fn parse_arcus_bbo(v: &serde_json::Value) -> (Option<f64>, Option<f64>) {
    (
        v.get("bestBid").and_then(|b| num_field(b, "price")),
        v.get("bestAsk").and_then(|a| num_field(a, "price")),
    )
}

/// AsterDEX open interest in USD: `openInterest` is base units, valued
/// at the mark price (same conversion as the bulk parsers).
fn parse_asterdex_open_interest(v: &serde_json::Value, mark_px: Option<f64>) -> Option<f64> {
    match (num_field(v, "openInterest"), mark_px) {
        (Some(oi), Some(px)) => Some(oi * px),
        _ => None,
    }
}

/// AsterDEX `ticker/24hr` (all symbols, one call) -> symbol -> 24h volume
/// in USD (`quoteVolume` is already quote-currency notional).
fn parse_asterdex_volumes(v: &serde_json::Value) -> HashMap<String, f64> {
    let mut out = HashMap::new();
    if let Some(list) = v.as_array() {
        for t in list {
            if let (Some(s), Some(q)) = (str_field(t, "symbol"), num_field(t, "quoteVolume")) {
                out.insert(s.to_string(), q);
            }
        }
    }
    out
}

async fn fetch_l2book(
    http: &reqwest::Client,
    coin: &str,
) -> Result<(Option<f64>, Option<f64>), String> {
    let v = crate::net::read_json(|| {
        http.post(HL_INFO_URL)
            .json(&serde_json::json!({ "type": "l2Book", "coin": coin }))
    })
    .await?;
    Ok(parse_l2book(&v))
}

async fn fetch_lighter_book_orders(
    http: &reqwest::Client,
    network: LighterNetwork,
    market_id: u64,
) -> Result<(Option<f64>, Option<f64>), String> {
    let url = format!(
        "{}/api/v1/orderBookOrders?market_id={market_id}&limit=2",
        network.rest_base()
    );
    let v = get_json(http, &url).await?;
    Ok(parse_lighter_book_orders(&v))
}

async fn fetch_arcus_bbo(
    http: &reqwest::Client,
    symbol: &str,
) -> Result<(Option<f64>, Option<f64>), String> {
    let v = get_json(http, &format!("{ARCUS_REST_URL}/v1/bbo/{symbol}")).await?;
    Ok(parse_arcus_bbo(&v))
}

async fn fetch_asterdex_open_interest(
    http: &reqwest::Client,
    symbol: &str,
    mark_px: Option<f64>,
) -> Result<Option<f64>, String> {
    let v = get_json(
        http,
        &format!("{ASTER_REST_URL}/fapi/v1/openInterest?symbol={symbol}"),
    )
    .await?;
    Ok(parse_asterdex_open_interest(&v, mark_px))
}

async fn fetch_arcus(http: &reqwest::Client) -> Result<Vec<MarketInfo>, String> {
    let v = get_json(http, &format!("{ARCUS_REST_URL}/v1/markets")).await?;
    parse_arcus(&v, crate::adapters::now_ms())
}

/// Live fields for one symbol (mark price + funding), refreshed on the
/// short cycle for search hits only. Static specs never come from here.
#[derive(Clone, Copy, Default)]
struct LiveFields {
    mark_px: Option<f64>,
    funding_raw: Option<f64>,
    funding_1h: Option<f64>,
    funding_apr: Option<f64>,
}

struct LiveSnapshot {
    at: Instant,
    fields: HashMap<String, LiveFields>,
}

/// Project full bulk rows down to their live fields (mark + funding).
fn live_fields_of(rows: Vec<MarketInfo>) -> HashMap<String, LiveFields> {
    rows.into_iter()
        .map(|r| {
            (
                r.symbol,
                LiveFields {
                    mark_px: r.mark_px,
                    funding_raw: r.funding_rate_raw,
                    funding_1h: r.funding_1h,
                    funding_apr: r.funding_apr,
                },
            )
        })
        .collect()
}

/// AsterDEX live fields from `premiumIndex` alone (mark + last funding
/// rate). Intervals come from the cached `fundingInfo` map (last bulk
/// refresh); a symbol missing from it has an unknown interval, so its
/// 1h rate and APR stay `None` — same rule as the bulk parser.
async fn fetch_live_asterdex(
    http: &reqwest::Client,
    intervals: &HashMap<String, f64>,
) -> Result<HashMap<String, LiveFields>, String> {
    let v = get_json(http, &format!("{ASTER_REST_URL}/fapi/v1/premiumIndex")).await?;
    let mut out = HashMap::new();
    if let Some(list) = v.as_array() {
        for p in list {
            let Some(symbol) = str_field(p, "symbol") else {
                continue;
            };
            let raw = num_field(p, "lastFundingRate");
            let interval = intervals.get(symbol).copied().filter(|h| *h > 0.0);
            let funding_1h = match (raw, interval) {
                (Some(r), Some(h)) => Some(funding_1h_from(r, h)),
                _ => None,
            };
            out.insert(
                symbol.to_string(),
                LiveFields {
                    mark_px: num_field(p, "markPrice"),
                    funding_raw: raw,
                    funding_1h,
                    funding_apr: funding_1h.map(funding_apr_pct),
                },
            );
        }
    }
    Ok(out)
}

pub struct MarketCatalog {
    http: reqwest::Client,
    bus: EventBus,
    data: RwLock<HashMap<(String, String), MarketInfo>>,
    last_errors: Mutex<HashMap<String, String>>,
    aster_specs: RwLock<HashMap<String, AsterSpec>>,
    aster_specs_at: Mutex<Option<Instant>>,
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
    /// Per-exchange live snapshots (mark + funding for any symbol),
    /// refreshed at most once per [`LIVE_TTL`].
    live_snapshots: Mutex<HashMap<String, LiveSnapshot>>,
    /// Last-known AsterDEX funding intervals from `fundingInfo`
    /// (per-symbol hours). The live funding math reads this; on a
    /// fundingInfo failure the previous map is kept, and a symbol never
    /// seen in it has an unknown interval (no default).
    aster_intervals: RwLock<HashMap<String, f64>>,
}

impl MarketCatalog {
    pub fn new(bus: EventBus) -> Self {
        Self {
            http: shared_http_client(),
            bus,
            data: RwLock::new(HashMap::new()),
            last_errors: Mutex::new(HashMap::new()),
            aster_specs: RwLock::new(HashMap::new()),
            aster_specs_at: Mutex::new(None),
            fetched_at: Mutex::new(HashMap::new()),
            last_success_at: Mutex::new(None),
            refresh_locks: Mutex::new(HashMap::new()),
            enrich_sem: tokio::sync::Semaphore::new(ENRICH_CONCURRENCY),
            live_snapshots: Mutex::new(HashMap::new()),
            aster_intervals: RwLock::new(HashMap::new()),
        }
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

    fn store(&self, exchange_id: &str, rows: Vec<MarketInfo>) {
        let mut data = self.data.write().unwrap_or_else(|e| e.into_inner());
        data.retain(|(ex, _), _| ex != exchange_id);
        for row in rows {
            data.insert((row.exchange_id.clone(), row.symbol.clone()), row);
        }
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
                self.store(exchange_id, rows);
                self.mark_fetched(exchange_id);
                true
            }
            Err(e) => {
                self.log_refresh_error(exchange_id, format!("{exchange_id} 全量行情刷新失败：{e}"));
                false
            }
        }
    }

    /// Fetch one exchange's bulk rows under that exchange's refresh lock,
    /// so two concurrent refreshes of the same exchange serialize while
    /// different exchanges still run concurrently. Freshness is re-checked
    /// under the lock: a concurrent refresh may have just finished while
    /// this task was waiting. Returns true when rows were stored.
    async fn refresh_one(&self, exchange_id: &str, force: bool) -> bool {
        let lock = self.refresh_lock(exchange_id);
        let _guard = lock.lock().await;
        if !force && self.is_fresh(exchange_id) {
            return false;
        }
        match exchange_id {
            "hyperliquid" => self.apply(exchange_id, fetch_hyperliquid(&self.http, None).await),
            "entropy" => self.apply(exchange_id, fetch_hyperliquid(&self.http, Some("io")).await),
            "lighter" => self.apply(
                exchange_id,
                fetch_lighter(&self.http, LighterNetwork::Mainnet).await,
            ),
            "lighter_robinhood" => self.apply(
                exchange_id,
                fetch_lighter(&self.http, LighterNetwork::Robinhood).await,
            ),
            "arcus" => self.apply(exchange_id, fetch_arcus(&self.http).await),
            "asterdex" => self.refresh_asterdex().await,
            _ => false,
        }
    }

    /// AsterDEX bulk refresh (runs under the asterdex refresh lock):
    /// specs (at most hourly) + premiumIndex + bookTicker + fundingInfo +
    /// ticker/24hr. fundingInfo is optional: on failure the last-known
    /// intervals are kept and symbols never seen stay unknown (no
    /// defaulted 8h); ticker/24hr is optional the same way. Only the
    /// premiumIndex/bookTicker pair is required to build rows.
    async fn refresh_asterdex(&self) -> bool {
        let specs_stale = {
            let at = self
                .aster_specs_at
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            match *at {
                None => true,
                Some(t) => t.elapsed() > ASTER_SPECS_TTL,
            }
        };
        if specs_stale {
            match get_json(
                &self.http,
                &format!("{ASTER_REST_URL}/fapi/v1/exchangeInfo"),
            )
            .await
            {
                Ok(v) => {
                    let specs = aster_specs_from(&v);
                    {
                        let mut slot = self.aster_specs.write().unwrap_or_else(|e| e.into_inner());
                        *slot = specs;
                    }
                    {
                        let mut at = self
                            .aster_specs_at
                            .lock()
                            .unwrap_or_else(|e| e.into_inner());
                        *at = Some(Instant::now());
                    }
                }
                Err(e) => {
                    self.log_refresh_error("asterdex", format!("asterdex 全量行情刷新失败：{e}"));
                    return false;
                }
            }
        }
        let specs = {
            let slot = self.aster_specs.read().unwrap_or_else(|e| e.into_inner());
            slot.clone()
        };
        let premium_url = format!("{ASTER_REST_URL}/fapi/v1/premiumIndex");
        let book_url = format!("{ASTER_REST_URL}/fapi/v1/ticker/bookTicker");
        let funding_info_url = format!("{ASTER_REST_URL}/fapi/v1/fundingInfo");
        let ticker_url = format!("{ASTER_REST_URL}/fapi/v1/ticker/24hr");
        let (premium, book, funding_info, ticker24) = tokio::join!(
            get_json(&self.http, &premium_url),
            get_json(&self.http, &book_url),
            get_json(&self.http, &funding_info_url),
            get_json(&self.http, &ticker_url),
        );
        // fundingInfo refreshes the cached interval map; on failure the
        // previous map is kept (intervals change rarely), and a symbol
        // never seen in it stays unknown.
        match funding_info {
            Ok(v) => {
                self.log_refresh_ok("asterdex-fundingInfo");
                if let Some(list) = v.as_array() {
                    let mut intervals: HashMap<String, f64> = HashMap::new();
                    for f in list {
                        if let (Some(s), Some(h)) = (
                            str_field(f, "symbol"),
                            num_field(f, "fundingIntervalHours"),
                        ) {
                            intervals.insert(s.to_string(), h);
                        }
                    }
                    let mut slot = self
                        .aster_intervals
                        .write()
                        .unwrap_or_else(|e| e.into_inner());
                    *slot = intervals;
                }
            }
            Err(e) => self.log_refresh_error(
                "asterdex-fundingInfo",
                format!("AsterDEX fundingInfo 刷新失败，费率周期沿用上次结果，未知标的年化暂不计算：{e}"),
            ),
        }
        let volumes = match ticker24 {
            Ok(v) => {
                self.log_refresh_ok("asterdex-ticker24hr");
                parse_asterdex_volumes(&v)
            }
            Err(e) => {
                self.log_refresh_error(
                    "asterdex-ticker24hr",
                    format!("AsterDEX ticker/24hr 刷新失败，24h成交额暂无：{e}"),
                );
                HashMap::new()
            }
        };
        let intervals = {
            let slot = self
                .aster_intervals
                .read()
                .unwrap_or_else(|e| e.into_inner());
            slot.clone()
        };
        let parsed = match (premium, book) {
            (Ok(p), Ok(b)) => parse_asterdex(
                &specs,
                &p,
                &b,
                &intervals,
                &volumes,
                crate::adapters::now_ms(),
            ),
            (Err(e), _) | (_, Err(e)) => Err(format!("asterdex 全量行情刷新失败：{e}")),
        };
        self.apply("asterdex", parsed)
    }

    /// Fetch (concurrently) the requested exchanges whose cache is stale
    /// (older than [`CACHE_TTL`]) — or all of them when `force` is set
    /// (manual refresh). Each exchange refreshes under its own lock, so
    /// concurrent callers cannot run two fetches for the same exchange.
    /// A single exchange failing leaves its last good rows in place and
    /// logs a warning. Unknown ids are ignored. `last_success_at` moves
    /// only when at least one exchange actually succeeded.
    pub async fn refresh_exchanges(&self, exchange_ids: &[String], force: bool) {
        use futures_util::future::join_all;
        let wanted: Vec<&str> = KNOWN_EXCHANGES
            .iter()
            .copied()
            .filter(|id| exchange_ids.iter().any(|x| x == id))
            .collect();
        let todo: Vec<&str> = wanted
            .into_iter()
            .filter(|id| force || !self.is_fresh(id))
            .collect();
        if todo.is_empty() {
            return;
        }
        let oks = join_all(todo.iter().map(|id| self.refresh_one(id, force))).await;
        if oks.iter().any(|o| *o) {
            let mut last = self
                .last_success_at
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *last = Some(crate::adapters::now_ms());
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

    /// Refresh the live fields (mark price + funding) of exactly the rows
    /// a search returned, at most once per [`LIVE_TTL`] per exchange. The
    /// static catalog keeps its 30s bulk cache — only these dynamic fields
    /// tick on the short cycle, so a watched search stays fresh without
    /// refetching specs or leverage. Each exchange's live fetch runs under
    /// that exchange's refresh lock (serial with its bulk refresh).
    ///
    /// A failed live fetch leaves the rows with their bulk values (never
    /// blanked) and logs one deduped warning; a stale snapshot is never
    /// applied as if it were fresh — only a snapshot inside [`LIVE_TTL`]
    /// touches a row, and a touched row's `updated_at` moves to now.
    pub async fn refresh_live_rows(&self, rows: &mut [MarketInfo]) {
        let now_ms = crate::adapters::now_ms();
        // Owned ids: the snapshot-apply step below mutates `rows`, so the
        // exchange list cannot borrow from it.
        let mut exchanges: Vec<String> = Vec::new();
        for row in rows.iter() {
            if !exchanges.contains(&row.exchange_id) {
                exchanges.push(row.exchange_id.clone());
            }
        }
        for exchange_id in exchanges {
            let lock = self.refresh_lock(&exchange_id);
            let _guard = lock.lock().await;
            let snapshot_fresh = self
                .live_snapshots
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&exchange_id)
                .map(|s| s.at.elapsed() < LIVE_TTL)
                .unwrap_or(false);
            if !snapshot_fresh {
                let intervals = {
                    let slot = self
                        .aster_intervals
                        .read()
                        .unwrap_or_else(|e| e.into_inner());
                    slot.clone()
                };
                let fetched = match exchange_id.as_str() {
                    "hyperliquid" => fetch_hyperliquid(&self.http, None)
                        .await
                        .map(live_fields_of),
                    "entropy" => fetch_hyperliquid(&self.http, Some("io"))
                        .await
                        .map(live_fields_of),
                    "lighter" => fetch_lighter(&self.http, LighterNetwork::Mainnet)
                        .await
                        .map(live_fields_of),
                    "lighter_robinhood" => fetch_lighter(&self.http, LighterNetwork::Robinhood)
                        .await
                        .map(live_fields_of),
                    "asterdex" => fetch_live_asterdex(&self.http, &intervals).await,
                    "arcus" => fetch_arcus(&self.http).await.map(live_fields_of),
                    _ => continue,
                };
                match fetched {
                    Ok(fields) => {
                        let mut live = self
                            .live_snapshots
                            .lock()
                            .unwrap_or_else(|e| e.into_inner());
                        live.insert(
                            exchange_id.clone(),
                            LiveSnapshot {
                                at: Instant::now(),
                                fields,
                            },
                        );
                    }
                    Err(e) => self.log_refresh_error(
                        &format!("live-{exchange_id}"),
                        format!("{exchange_id} 实时数据刷新失败：{e}"),
                    ),
                }
            }
            // Apply only a fresh snapshot: a failed fetch must not dress
            // stale fields up as current data.
            let fields = {
                let live = self
                    .live_snapshots
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                match live.get(&exchange_id) {
                    Some(s) if s.at.elapsed() < LIVE_TTL => Some(s.fields.clone()),
                    _ => None,
                }
            };
            let Some(fields) = fields else {
                continue;
            };
            for row in rows.iter_mut().filter(|r| r.exchange_id == exchange_id) {
                let Some(lf) = fields.get(&row.symbol) else {
                    continue;
                };
                let mut touched = false;
                if let Some(px) = lf.mark_px {
                    row.mark_px = Some(px);
                    touched = true;
                }
                // Funding is replaced as a group only when the live fetch
                // saw a raw rate; its 1h/APR may honestly be None (unknown
                // AsterDEX interval) — that None is the truth, not a loss.
                if let Some(raw) = lf.funding_raw {
                    row.funding_rate_raw = Some(raw);
                    row.funding_1h = lf.funding_1h;
                    row.funding_apr = lf.funding_apr;
                    touched = true;
                }
                if touched {
                    row.updated_at = now_ms;
                }
            }
        }
    }

    /// Fill the per-symbol gaps the bulk endpoints cannot provide, for
    /// exactly the rows a search returned (never for the whole market):
    /// true BBO for Hyperliquid/Entropy (`l2Book`), Lighter/RH
    /// (`orderBookOrders`) and Arcus (`/v1/bbo/{market}`); AsterDEX open
    /// interest (`openInterest?symbol=..`, base units x mark price). The
    /// fills run concurrently but capped at [`ENRICH_CONCURRENCY`]
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
                match row.exchange_id.as_str() {
                    "hyperliquid" | "entropy" => {
                        match fetch_l2book(&self.http, &row.symbol).await {
                            Ok((bid, ask)) => {
                                if bid.is_some() {
                                    row.bid_px = bid;
                                }
                                if ask.is_some() {
                                    row.ask_px = ask;
                                }
                            }
                            Err(e) => self.log_refresh_error(
                                "bbo-hyperliquid",
                                format!("{} 真实盘口补拉失败：{e}", row.exchange_id),
                            ),
                        }
                    }
                    "lighter" | "lighter_robinhood" => {
                        let network = if row.exchange_id == "lighter_robinhood" {
                            LighterNetwork::Robinhood
                        } else {
                            LighterNetwork::Mainnet
                        };
                        if let Some(mid) = row.market_id {
                            match fetch_lighter_book_orders(&self.http, network, mid).await {
                                Ok((bid, ask)) => {
                                    if bid.is_some() {
                                        row.bid_px = bid;
                                    }
                                    if ask.is_some() {
                                        row.ask_px = ask;
                                    }
                                    // Bulk mid was last-trade price; the
                                    // true mid from the filled BBO is better.
                                    if let (Some(b), Some(a)) = (row.bid_px, row.ask_px) {
                                        row.mid_px = Some((b + a) / 2.0);
                                    }
                                }
                                Err(e) => self.log_refresh_error(
                                    "bbo-lighter",
                                    format!("{} 真实盘口补拉失败：{e}", row.exchange_id),
                                ),
                            }
                        }
                    }
                    "arcus" => match fetch_arcus_bbo(&self.http, &row.symbol).await {
                        Ok((bid, ask)) => {
                            if bid.is_some() {
                                row.bid_px = bid;
                            }
                            if ask.is_some() {
                                row.ask_px = ask;
                            }
                            if let (Some(b), Some(a)) = (row.bid_px, row.ask_px) {
                                row.mid_px = Some((b + a) / 2.0);
                            }
                        }
                        Err(e) => self
                            .log_refresh_error("bbo-arcus", format!("Arcus 真实盘口补拉失败：{e}")),
                    },
                    "asterdex" => {
                        if row.open_interest_usd.is_none() {
                            match fetch_asterdex_open_interest(&self.http, &row.symbol, row.mark_px)
                                .await
                            {
                                Ok(oi) => row.open_interest_usd = oi,
                                Err(e) => self.log_refresh_error(
                                    "oi-asterdex",
                                    format!("AsterDEX 持仓量补拉失败：{e}"),
                                ),
                            }
                        }
                    }
                    _ => {}
                }
                row
            })
            .collect();
        join_all(futs).await
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

    fn json(s: &str) -> serde_json::Value {
        serde_json::from_str(s).unwrap()
    }

    #[test]
    fn normalize_base_symbol_cases() {
        assert_eq!(normalize_base_symbol("BTC"), "BTC");
        assert_eq!(normalize_base_symbol("io:NVDA"), "NVDA");
        assert_eq!(normalize_base_symbol("BTCUSDT"), "BTC");
        assert_eq!(normalize_base_symbol("ETHUSDC"), "ETH");
        assert_eq!(normalize_base_symbol("BTC-USD"), "BTC");
        assert_eq!(normalize_base_symbol("NVDA/USDG"), "NVDA");
        assert_eq!(normalize_base_symbol("rhSPY/USDC"), "SPY");
        assert_eq!(normalize_base_symbol("1000BONK"), "1000BONK");
        assert_eq!(normalize_base_symbol("USDCAD"), "USDCAD");
        assert_eq!(normalize_base_symbol("kPEPEUSDT"), "KPEPE");
        assert_eq!(normalize_base_symbol("SPY"), "SPY");
        assert_eq!(normalize_base_symbol("BTC/USD"), "BTC");
    }

    #[test]
    fn funding_conversions() {
        // AsterDEX 8h raw 0.0008 -> 1h 0.0001, APR 87.6%.
        let one_h = funding_1h_from(0.0008, 8.0);
        assert!((one_h - 0.0001).abs() < 1e-12);
        assert!((funding_apr_pct(one_h) - 87.6).abs() < 1e-9);
        // Arcus/HL 1h raw 0.0000125 -> APR 10.95%.
        assert!((funding_apr_pct(0.0000125) - 10.95).abs() < 1e-9);
        // Lighter 8h-equivalent 7.2e-05 -> 1h 9e-06.
        assert!((funding_1h_from(7.2e-05, 8.0) - 9e-06).abs() < 1e-12);
        // AsterDEX 4h raw 0.0004 -> 1h 0.0001 (same APR as the 8h case).
        assert!((funding_1h_from(0.0004, 4.0) - 0.0001).abs() < 1e-12);
        // AsterDEX 1h raw is already hourly.
        assert!((funding_1h_from(0.00008, 1.0) - 0.00008).abs() < 1e-12);
    }

    /// Each venue's raw funding rate -> 1h rate, with the venue's real
    /// quote period (HL/Entropy/Arcus 1h, Lighter 8h-equivalent,
    /// AsterDEX per-symbol 1/2/4/8h). A venue converted with the wrong
    /// period is off by 2x-8x, so this pins the mapping, not just the math.
    #[test]
    fn funding_1h_per_venue() {
        // Hyperliquid BTC: hourly raw 0.0000069845 stays as-is.
        assert!((funding_1h_from(0.0000069845, 1.0) - 0.0000069845).abs() < 1e-12);
        // Lighter BTC: 8h-equivalent 0.000096 -> 1h 0.000012.
        assert!((funding_1h_from(0.000096, 8.0) - 0.000012).abs() < 1e-12);
        // Arcus BTC: hourly raw 0.0000125 stays as-is (APR 10.95%).
        assert!((funding_apr_pct(funding_1h_from(0.0000125, 1.0)) - 10.95).abs() < 1e-9);
        // AsterDEX BTC (8h market): raw 0.00009616 -> 1h 0.00001202.
        assert!((funding_1h_from(0.00009616, 8.0) - 0.00001202).abs() < 1e-12);
        // AsterDEX ETH (1h market): raw 0.00008 stays as-is.
        assert!((funding_1h_from(0.00008, 1.0) - 0.00008).abs() < 1e-12);
        // AsterDEX 2h market: raw 0.0002 -> 1h 0.0001.
        assert!((funding_1h_from(0.0002, 2.0) - 0.0001).abs() < 1e-12);
    }

    #[test]
    fn parse_l2book_cases() {
        let v = json(
            r#"{"coin":"BTC","time":1795633991698,"levels":[
                [{"px":"59957.0","sz":"6.77946","n":26},{"px":"59956.0","sz":"0.08","n":1}],
                [{"px":"59958.0","sz":"1.5","n":3},{"px":"59959.0","sz":"0.2","n":1}]
            ]}"#,
        );
        assert_eq!(parse_l2book(&v), (Some(59957.0), Some(59958.0)));
        // Empty sides stay None, never 0.
        let empty = json(r#"{"coin":"THIN","time":0,"levels":[[],[]]}"#);
        assert_eq!(parse_l2book(&empty), (None, None));
        // Malformed response -> both None.
        assert_eq!(parse_l2book(&json(r#"{"error":"bad"}"#)), (None, None));
    }

    #[test]
    fn parse_lighter_book_orders_cases() {
        let v = json(
            r#"{"code":200,"total_asks":2,"asks":[
                {"price":"59876.2","remaining_base_amount":"0.00193"},
                {"price":"59877.7","remaining_base_amount":"0.05010"}],
               "total_bids":2,"bids":[
                {"price":"59875.9","remaining_base_amount":"0.01"},
                {"price":"59874.0","remaining_base_amount":"0.5"}]}"#,
        );
        assert_eq!(
            parse_lighter_book_orders(&v),
            (Some(59875.9), Some(59876.2))
        );
        assert_eq!(
            parse_lighter_book_orders(&json(r#"{"code":200,"asks":[],"bids":[]}"#)),
            (None, None)
        );
    }

    #[test]
    fn parse_arcus_bbo_cases() {
        let v = json(
            r#"{"bestBid":{"price":"59809","size":"0.05251135"},
                "bestAsk":{"price":"59809.1","size":"3.23288548"},
                "lastSequenceId":972906112}"#,
        );
        assert_eq!(parse_arcus_bbo(&v), (Some(59809.0), Some(59809.1)));
        assert_eq!(
            parse_arcus_bbo(&json(r#"{"bestBid":null,"bestAsk":null}"#)),
            (None, None)
        );
    }

    #[test]
    fn parse_asterdex_open_interest_cases() {
        let v = json(r#"{"symbol":"BTCUSDT","openInterest":"6091.502","time":1795633911549}"#);
        // 6091.502 base units x $59940.4 mark.
        let oi = parse_asterdex_open_interest(&v, Some(59940.4)).unwrap();
        assert!((oi - 6091.502 * 59940.4).abs() < 1.0);
        assert_eq!(parse_asterdex_open_interest(&v, None), None);
    }

    #[test]
    fn parse_asterdex_volumes_cases() {
        let v = json(
            r#"[{"symbol":"BTCUSDT","quoteVolume":"587866473.21"},
                {"symbol":"ETHUSDT","quoteVolume":"123456.0"},
                {"symbol":"NOPEUSDT"}]"#,
        );
        let map = parse_asterdex_volumes(&v);
        assert_eq!(map.get("BTCUSDT"), Some(&587866473.21));
        assert_eq!(map.get("ETHUSDT"), Some(&123456.0));
        assert!(!map.contains_key("NOPEUSDT"));
    }

    #[test]
    fn hyperliquid_tick_size_cases() {
        // Live-verified: BTC ~$58k -> $1.00, ETH ~$1641 -> $0.10.
        assert!((hyperliquid_tick_size(58_000.0, 5) - 1.0).abs() < 1e-12);
        assert!((hyperliquid_tick_size(1641.0, 3) - 0.1).abs() < 1e-12);
        assert!((hyperliquid_tick_size(143.0, 3) - 0.01).abs() < 1e-12);
        // Non-positive price falls back to the decimals constraint:
        // szDecimals 5 -> at most 1 price decimal -> tick 0.1.
        assert!((hyperliquid_tick_size(0.0, 5) - 0.1).abs() < 1e-12);
    }

    #[test]
    fn next_hour_utc_ms_cases() {
        // 2026-11-25 15:44:00 UTC -> 16:00:00 UTC.
        let now = 1_795_621_440_000i64;
        assert_eq!(next_hour_utc_ms(now), 1_795_622_400_000i64);
        // Exactly on the hour -> the *next* hour.
        assert_eq!(next_hour_utc_ms(1_795_622_400_000), 1_795_626_000_000);
    }

    #[test]
    fn parse_hyperliquid_fixture() {
        let v = json(
            r#"[{"universe":[
                {"name":"BTC","szDecimals":5,"maxLeverage":40},
                {"name":"io:NVDA","szDecimals":3,"maxLeverage":10},
                {"name":"DEAD","szDecimals":2,"maxLeverage":5,"isDelisted":true},
                {"name":"THIN","szDecimals":2,"maxLeverage":5}
            ]},[
                {"markPx":"58033.0","midPx":"58031.5","oraclePx":"58056.0","funding":"0.0000069845","impactPxs":["58031.0","58032.0"],"openInterest":"34413.96742","dayNtlVlm":"3411439784.874"},
                {"markPx":"1206.8","midPx":"1204.75","oraclePx":"1206.7","funding":"0.0000015625","impactPxs":["1203.597","1208.217"]},
                {"markPx":"1.0","midPx":"1.0","oraclePx":"1.0","funding":"0.0","impactPxs":["1.0","1.0"]},
                {"markPx":"10.0","midPx":null,"oraclePx":"10.0","funding":"0.0","impactPxs":null}
            ]]"#,
        );
        let rows = parse_hyperliquid(&v, "hyperliquid", 1_795_622_400_000).unwrap();
        // Delisted coin skipped; the other three kept.
        assert_eq!(rows.len(), 3);
        let btc = &rows[0];
        assert_eq!(btc.symbol, "BTC");
        assert_eq!(btc.base_symbol, "BTC");
        assert_eq!(btc.settlement_currency, "USDC");
        assert_eq!(btc.max_leverage, Some(40.0));
        assert_eq!(btc.tick_size, Some(1.0));
        assert_eq!(btc.step_size, Some(0.00001));
        assert_eq!(btc.min_notional, Some(10.0));
        // Bulk has no bid/ask: impactPxs is an execution-price proxy,
        // never the BBO — it stays None until the per-hit l2Book fill.
        assert_eq!(btc.bid_px, None);
        assert_eq!(btc.ask_px, None);
        assert_eq!(btc.mark_px, Some(58033.0));
        assert_eq!(btc.index_px, Some(58056.0));
        assert_eq!(btc.funding_interval_hours, Some(1.0));
        assert_eq!(btc.funding_1h, Some(0.0000069845));
        assert_eq!(btc.next_funding_time, Some(1_795_626_000_000));
        assert_eq!(btc.volume_24h_usd, Some(3411439784.874));
        // OI is base units; OI USD = 34413.96742 * 58033.0.
        assert!((btc.open_interest_usd.unwrap() - 34413.96742 * 58033.0).abs() < 1.0);
        let nvda = &rows[1];
        assert_eq!(nvda.symbol, "io:NVDA");
        assert_eq!(nvda.base_symbol, "NVDA");
        let thin = &rows[2];
        assert_eq!(thin.symbol, "THIN");
        assert_eq!(thin.bid_px, None);
        assert_eq!(thin.mid_px, None);
    }

    #[test]
    fn parse_lighter_fixture() {
        let details = json(
            r#"{"code":200,"order_book_details":[
                {"symbol":"BTC","market_id":1,"market_type":"perp","status":"active",
                 "price_decimals":1,"size_decimals":5,"min_quote_amount":"10.000000",
                 "min_initial_margin_fraction":200,"mark_price":"58036.4","index_price":"58060.2","last_trade_price":58048.4,
                 "daily_quote_token_volume":913793455.94,"open_interest_usd":"224951137.406376"},
                {"symbol":"SOL","market_id":2,"market_type":"perp","status":"inactive",
                 "price_decimals":2,"size_decimals":3,"min_quote_amount":"10.000000",
                 "min_initial_margin_fraction":400,"mark_price":"100.0","index_price":"100.0","last_trade_price":100.0},
                {"symbol":"BTC/USDC","market_id":2049,"market_type":"spot","status":"active",
                 "price_decimals":1,"size_decimals":5,"min_quote_amount":"10.000000",
                 "min_initial_margin_fraction":200,"mark_price":"58036.4","index_price":"58060.2","last_trade_price":58048.4}
            ]}"#,
        );
        let funding = json(
            r#"{"code":200,"funding_rates":[
                {"market_id":1,"exchange":"lighter","symbol":"BTC","rate":0.000088},
                {"market_id":2,"exchange":"lighter","symbol":"SOL","rate":0.00005},
                {"market_id":1,"exchange":"other","symbol":"BTC","rate":0.5}
            ]}"#,
        );
        let rows = parse_lighter(&details, &funding, "lighter", "USDC", 1_795_622_400_000).unwrap();
        // Only the active perp survives; spot + inactive are dropped.
        assert_eq!(rows.len(), 1);
        let btc = &rows[0];
        assert_eq!(btc.symbol, "BTC");
        assert_eq!(btc.base_symbol, "BTC");
        assert_eq!(btc.market_id, Some(1));
        assert_eq!(btc.max_leverage, Some(50.0));
        assert_eq!(btc.tick_size, Some(0.1));
        assert_eq!(btc.step_size, Some(0.00001));
        assert_eq!(btc.min_notional, Some(10.0));
        assert_eq!(btc.bid_px, None);
        assert_eq!(btc.mid_px, Some(58048.4));
        // 8h-equivalent rate 0.000088 -> 1h 0.000011.
        assert!((btc.funding_1h.unwrap() - 0.000011).abs() < 1e-12);
        assert_eq!(btc.funding_interval_hours, Some(1.0));
        assert_eq!(btc.next_funding_time, Some(1_795_626_000_000));
        assert_eq!(btc.volume_24h_usd, Some(913793455.94));
        assert_eq!(btc.open_interest_usd, Some(224951137.406376));
    }

    #[test]
    fn parse_asterdex_fixture() {
        let ei = json(
            r#"{"symbols":[
                {"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","baseAsset":"BTC","marginAsset":"USDT",
                 "filters":[{"filterType":"PRICE_FILTER","tickSize":"0.1"},{"filterType":"LOT_SIZE","stepSize":"0.001"},{"filterType":"MIN_NOTIONAL","notional":"5"}]},
                {"symbol":"ETHUSDT","status":"TRADING","contractType":"PERPETUAL","baseAsset":"ETH","marginAsset":"USDT",
                 "filters":[{"filterType":"PRICE_FILTER","tickSize":"0.01"},{"filterType":"LOT_SIZE","stepSize":"0.001"},{"filterType":"MIN_NOTIONAL","notional":"5"}]},
                {"symbol":"OLDUSDT","status":"TRADING","contractType":"PERPETUAL","baseAsset":"OLD","marginAsset":"USDT","filters":[]}
            ]}"#,
        );
        let specs = aster_specs_from(&ei);
        assert_eq!(specs.len(), 3);
        let premium = json(
            r#"[{"symbol":"BTCUSDT","markPrice":"58038.2","indexPrice":"58059.68","lastFundingRate":"0.00009616","nextFundingTime":1795622400000,"time":1795621662000},
                {"symbol":"ETHUSDT","markPrice":"1641.0","indexPrice":"1641.5","lastFundingRate":"0.00008","nextFundingTime":1795622400000,"time":1795621662000},
                {"symbol":"STALEUSDT","markPrice":"1.0","indexPrice":"1.0","lastFundingRate":"0.0","nextFundingTime":1795622400000,"time":1795621662000}]"#,
        );
        let book = json(
            r#"[{"symbol":"BTCUSDT","bidPrice":"58093.3","askPrice":"58093.4"},
               {"symbol":"ETHUSDT","bidPrice":"1640.9","askPrice":"1641.1"}]"#,
        );
        // fundingInfo is per-symbol: BTC settles every 8h, ETH every 1h.
        let mut intervals = HashMap::new();
        intervals.insert("BTCUSDT".to_string(), 8.0);
        intervals.insert("ETHUSDT".to_string(), 1.0);
        // ticker/24hr volumes (quoteVolume).
        let mut volumes = HashMap::new();
        volumes.insert("BTCUSDT".to_string(), 587866473.21);
        let rows = parse_asterdex(
            &specs,
            &premium,
            &book,
            &intervals,
            &volumes,
            1_795_622_400_000,
        )
        .unwrap();
        // Stale premium entry (not in exchangeInfo) is ignored.
        assert_eq!(rows.len(), 2);
        let btc = &rows[0];
        assert_eq!(btc.exchange_id, "asterdex");
        assert_eq!(btc.base_symbol, "BTC");
        assert_eq!(btc.settlement_currency, "USDT");
        // No public source for max leverage — always null now that the
        // signed leverageBracket read (account credentials) is gone.
        assert_eq!(btc.max_leverage, None);
        assert_eq!(btc.volume_24h_usd, Some(587866473.21));
        assert_eq!(btc.tick_size, Some(0.1));
        assert_eq!(btc.step_size, Some(0.001));
        assert_eq!(btc.min_notional, Some(5.0));
        assert_eq!(btc.bid_px, Some(58093.3));
        assert_eq!(btc.ask_px, Some(58093.4));
        assert!((btc.mid_px.unwrap() - 58093.35).abs() < 1e-9);
        assert_eq!(btc.funding_interval_hours, Some(8.0));
        assert!((btc.funding_1h.unwrap() - 0.00009616 / 8.0).abs() < 1e-12);
        assert_eq!(btc.next_funding_time, Some(1795622400000));
        // Open interest is filled per search hit, not in the bulk parse.
        assert_eq!(btc.open_interest_usd, None);
        // ETH settles hourly: raw rate is already the 1h rate.
        let eth = &rows[1];
        assert_eq!(eth.funding_interval_hours, Some(1.0));
        assert_eq!(eth.funding_1h, Some(0.00008));
        // No volume row for ETH in this map -> null.
        assert_eq!(eth.max_leverage, None);
        assert_eq!(eth.volume_24h_usd, None);
    }

    /// Unknown funding interval (symbol missing from fundingInfo): the
    /// raw rate is kept, but the interval, the 1h rate and the APR are
    /// all None — no 8h default, no invented annualized number.
    #[test]
    fn parse_asterdex_unknown_interval_has_no_apr() {
        let ei = json(
            r#"{"symbols":[
                {"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","baseAsset":"BTC","marginAsset":"USDT",
                 "filters":[{"filterType":"PRICE_FILTER","tickSize":"0.1"},{"filterType":"LOT_SIZE","stepSize":"0.001"},{"filterType":"MIN_NOTIONAL","notional":"5"}]}
            ]}"#,
        );
        let specs = aster_specs_from(&ei);
        let premium = json(
            r#"[{"symbol":"BTCUSDT","markPrice":"58038.2","indexPrice":"58059.68","lastFundingRate":"0.00009616","nextFundingTime":1795622400000,"time":1795621662000}]"#,
        );
        let book = json(r#"[{"symbol":"BTCUSDT","bidPrice":"58093.3","askPrice":"58093.4"}]"#);
        // No intervals at all: fundingInfo failed or never arrived.
        let rows = parse_asterdex(
            &specs,
            &premium,
            &book,
            &HashMap::new(),
            &HashMap::new(),
            1_795_622_400_000,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let btc = &rows[0];
        assert_eq!(btc.funding_rate_raw, Some(0.00009616));
        assert_eq!(btc.funding_interval_hours, None);
        assert_eq!(btc.funding_1h, None);
        assert_eq!(btc.funding_apr, None);
    }

    #[test]
    fn parse_arcus_fixture() {
        let v = json(
            r#"{"markets":[
                {"marketDisplayName":"BTC-USD","status":"ONLINE","type":"PERPETUAL","baseAsset":"BTC",
                 "tickSize":"0.1","stepSize":"0.00000001","minOrderNotional":"5",
                 "oraclePrice":"58032.7","markPrice":"58004.7","fundingRate":"0.0000125","nextFundingAt":1795622400,
                 "initialMarginFraction":"0.025","offHoursInitialMarginFraction":"0.025","isOutsideRth":false,
                 "volume24hNotional":"35622895.88","openInterest":"130.87365841"},
                {"marketDisplayName":"AMD-USD","status":"ONLINE","type":"PERPETUAL","baseAsset":"AMD",
                 "tickSize":"0.01","stepSize":"0.001","minOrderNotional":"5",
                 "oraclePrice":"100.0","markPrice":"100.0","fundingRate":"0.00001","nextFundingAt":1795622400,
                 "initialMarginFraction":"0.1","offHoursInitialMarginFraction":"0.15","isOutsideRth":true},
                {"marketDisplayName":"DEAD-USD","status":"OFFLINE","type":"PERPETUAL","baseAsset":"DEAD",
                 "tickSize":"0.1","stepSize":"0.001","minOrderNotional":"5",
                 "oraclePrice":"1.0","markPrice":"1.0","fundingRate":"0.0","nextFundingAt":1795622400,
                 "initialMarginFraction":"0.1","isOutsideRth":false}
            ]}"#,
        );
        let rows = parse_arcus(&v, 1_795_622_400_000).unwrap();
        assert_eq!(rows.len(), 2);
        let btc = &rows[0];
        assert_eq!(btc.base_symbol, "BTC");
        assert_eq!(btc.settlement_currency, "USDG");
        assert_eq!(btc.max_leverage, Some(40.0));
        assert_eq!(btc.tick_size, Some(0.1));
        assert_eq!(btc.funding_1h, Some(0.0000125));
        assert_eq!(btc.funding_interval_hours, Some(1.0));
        // nextFundingAt is seconds -> ms.
        assert_eq!(btc.next_funding_time, Some(1795622400000));
        assert_eq!(btc.bid_px, None);
        assert_eq!(btc.volume_24h_usd, Some(35622895.88));
        // OI is base units; OI USD = 130.87365841 * 58004.7.
        assert!((btc.open_interest_usd.unwrap() - 130.87365841 * 58004.7).abs() < 1.0);
        // Off-hours RWA uses offHoursInitialMarginFraction: 1/0.15.
        let amd = &rows[1];
        assert!((amd.max_leverage.unwrap() - 1.0 / 0.15).abs() < 1e-9);
    }

    #[test]
    fn search_matches_base_and_symbol_and_empty() {
        let catalog = MarketCatalog::new(EventBus::new(16));
        let all: Vec<String> = KNOWN_EXCHANGES.iter().map(|s| s.to_string()).collect();
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
}
