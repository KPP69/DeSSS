//! Market rows + shared catalog parse helpers (PR-1.3 commit 3).
//! Moved from `adapters/market_catalog.rs`: the row types
//! (`MarketInfo` / `Instrument` / `Quote`) and the JSON/number
//! helpers every venue catalog shares. `MarketCatalog` itself
//! (cache, freshness, refresh, search, enrich, persistence) stays
//! in `adapters/market_catalog.rs`.

use serde::{Deserialize, Serialize};

use crate::core::symbol::normalize_base_symbol;

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

/// A market's static contract: everything that does not move between
/// refreshes. This is what SQLite `instruments` persists — a restart
/// reloads it with no network, so the first search is local. `dex` is
/// derived from the exchange (`entropy` -> `"io"`, else `None`);
/// `status` is `"trading"` for every parsed row: the parsers only keep
/// tradeable markets, so no exchange currently contributes another
/// status. `base_symbol` is NOT stored — it is derived from `symbol`
/// at assembly time. (No `dex` field: no runtime path reads it —
/// the venue registry carries the dex — so it is not persisted.)
#[derive(Clone, Debug, PartialEq)]
pub struct Instrument {
    pub exchange_id: String,
    pub symbol: String,
    pub tick_size: Option<f64>,
    pub step_size: Option<f64>,
    pub min_notional: Option<f64>,
    pub max_leverage: Option<f64>,
    pub funding_interval_h: Option<f64>,
    pub settlement_currency: String,
    pub market_id: Option<u64>,
    pub status: String,
    pub updated_at: i64,
}

/// A market's live fields: everything a refresh or a WS frame can
/// move. Memory-only by design — quotes are never persisted. `bbo`
/// is `(bid_px, ask_px)`; `change_24h` has no bulk source on any venue
/// yet (AsterDEX `ticker/24hr` carries `priceChangePercent` — a later
/// wiring), so it is `None` for now rather than invented.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Quote {
    pub mark_px: Option<f64>,
    pub index_px: Option<f64>,
    pub bid_px: Option<f64>,
    pub ask_px: Option<f64>,
    pub mid_px: Option<f64>,
    pub funding_rate_raw: Option<f64>,
    pub funding_1h: Option<f64>,
    pub funding_apr: Option<f64>,
    pub next_funding_time: Option<i64>,
    pub volume_24h_usd: Option<f64>,
    pub open_interest_usd: Option<f64>,
    pub change_24h: Option<f64>,
}

impl MarketInfo {
    /// Split a row into its static half (persisted) and live half
    /// (memory-only). The split is lossless: assembling the pair
    /// returns the original row.
    pub fn split(&self) -> (Instrument, Quote) {
        (
            Instrument {
                exchange_id: self.exchange_id.clone(),
                symbol: self.symbol.clone(),
                tick_size: self.tick_size,
                step_size: self.step_size,
                min_notional: self.min_notional,
                max_leverage: self.max_leverage,
                funding_interval_h: self.funding_interval_hours,
                settlement_currency: self.settlement_currency.clone(),
                market_id: self.market_id,

                status: "trading".to_string(),
                updated_at: self.updated_at,
            },
            Quote {
                mark_px: self.mark_px,
                index_px: self.index_px,
                bid_px: self.bid_px,
                ask_px: self.ask_px,
                mid_px: self.mid_px,
                funding_rate_raw: self.funding_rate_raw,
                funding_1h: self.funding_1h,
                funding_apr: self.funding_apr,
                next_funding_time: self.next_funding_time,
                volume_24h_usd: self.volume_24h_usd,
                open_interest_usd: self.open_interest_usd,
                change_24h: None,
            },
        )
    }

    /// Reassemble a row from its static half and a live half
    /// (empty after a cold start — prices show "—" until a refresh
    /// or a WS frame fills them).
    pub fn assemble(ins: &Instrument, q: &Quote) -> MarketInfo {
        MarketInfo {
            exchange_id: ins.exchange_id.clone(),
            symbol: ins.symbol.clone(),
            base_symbol: normalize_base_symbol(&ins.symbol),
            market_id: ins.market_id,
            settlement_currency: ins.settlement_currency.clone(),
            max_leverage: ins.max_leverage,
            tick_size: ins.tick_size,
            step_size: ins.step_size,
            min_notional: ins.min_notional,
            bid_px: q.bid_px,
            ask_px: q.ask_px,
            mid_px: q.mid_px,
            mark_px: q.mark_px,
            index_px: q.index_px,
            funding_rate_raw: q.funding_rate_raw,
            funding_interval_hours: ins.funding_interval_h,
            funding_1h: q.funding_1h,
            funding_apr: q.funding_apr,
            next_funding_time: q.next_funding_time,
            volume_24h_usd: q.volume_24h_usd,
            open_interest_usd: q.open_interest_usd,
            updated_at: ins.updated_at,
        }
    }
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

/// A JSON number-or-numeric-string as f64; missing / non-numeric reads as
/// None (unlike `f64_from`, which defaults to 0.0 — wrong for optional
/// market fields, where absence must stay absent).
pub(crate) fn opt_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn opt_str<'a>(v: &'a serde_json::Value) -> Option<&'a str> {
    v.as_str()
}

pub(crate) fn str_field<'a>(v: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(opt_str)
}

pub(crate) fn num_field(v: &serde_json::Value, key: &str) -> Option<f64> {
    v.get(key).and_then(opt_f64)
}

pub(crate) async fn get_json(
    http: &reqwest::Client,
    url: &str,
) -> Result<serde_json::Value, String> {
    // One retry on a stale pooled connection (SendRequest /
    // UnexpectedEof); error text is redacted (no URL query params).
    crate::net::read_json(|| http.get(url)).await
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn next_hour_utc_ms_cases() {
        // 2026-11-25 15:44:00 UTC -> 16:00:00 UTC.
        let now = 1_795_621_440_000i64;
        assert_eq!(next_hour_utc_ms(now), 1_795_622_400_000i64);
        // Exactly on the hour -> the *next* hour.
        assert_eq!(next_hour_utc_ms(1_795_622_400_000), 1_795_626_000_000);
    }

    /// PR-0.3: the Instrument/Quote split is lossless — assembling the
    /// pair returns the original row, field for field, on both the
    /// plain and the `io:`-prefixed (Entropy) shapes.
    #[test]
    fn split_assemble_round_trip_loses_nothing() {
        for (exchange_id, symbol, base_symbol) in [
            ("hyperliquid", "BTC", "BTC"),
            ("entropy", "io:NVDA", "NVDA"),
        ] {
            let row = MarketInfo {
                exchange_id: exchange_id.into(),
                symbol: symbol.into(),
                base_symbol: base_symbol.into(),
                market_id: Some(7),
                settlement_currency: "USDC".into(),
                max_leverage: Some(40.0),
                tick_size: Some(0.5),
                step_size: Some(0.001),
                min_notional: Some(10.0),
                bid_px: Some(58000.0),
                ask_px: Some(58001.0),
                mid_px: Some(58000.5),
                mark_px: Some(58000.5),
                index_px: Some(58000.0),
                funding_rate_raw: Some(0.0008),
                funding_interval_hours: Some(1.0),
                funding_1h: Some(0.0008),
                funding_apr: Some(700.8),
                next_funding_time: Some(1_800_000_000_000),
                volume_24h_usd: Some(1_234_567.0),
                open_interest_usd: Some(9_876_543.0),
                updated_at: 1_700_000_000_000,
            };
            let (ins, quote) = row.split();
            assert_eq!(MarketInfo::assemble(&ins, &quote), row);
        }
    }
}
