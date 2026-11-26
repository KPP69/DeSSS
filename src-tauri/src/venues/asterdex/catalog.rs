//! AsterDEX catalog (PR-1.3 commit 2): the 5 /fapi/v3/* public
//! endpoints (exchangeInfo, premiumIndex, ticker/bookTicker,
//! fundingInfo, ticker/24hr). Moved verbatim from
//! `adapters/market_catalog.rs` except `fetch_asterdex`, which is
//! the old `refresh_asterdex` re-homed here with its specs/interval
//! caches (see below).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::ASTER_REST_URL;
use crate::core::market::{
    funding_1h_from, funding_apr_pct, get_json, num_field, str_field, MarketInfo,
};
use crate::core::symbol::normalize_base_symbol;
use crate::core::util::now_ms;

const ASTER_SPECS_TTL: Duration = Duration::from_secs(3600);

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
            // 24h volume from ticker/24hr (quoteVolume, USDT). V3 has
            // no openInterest endpoint, so open interest stays None.
            volume_24h_usd: volumes.get(symbol).copied(),
            open_interest_usd: None,
            updated_at: now_ms,
        });
    }
    Ok(out)
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

/// Specs (exchangeInfo) are cached at most hourly and funding
/// intervals are cached across refreshes. The caches live here as
/// module statics now — one MarketCatalog instance owned them
/// before, and the venue is a process-wide singleton the same way.
static SPECS: Mutex<Option<HashMap<String, AsterSpec>>> = Mutex::new(None);
static SPECS_AT: Mutex<Option<Instant>> = Mutex::new(None);
static INTERVALS: Mutex<Option<HashMap<String, f64>>> = Mutex::new(None);

/// The full bulk refresh: specs (at most hourly) + premiumIndex +
/// bookTicker + fundingInfo + ticker/24hr. fundingInfo is optional:
/// on failure the last-known intervals are kept and symbols never
/// seen stay unknown (no defaulted 8h); ticker/24hr is optional the
/// same way. Only the premiumIndex/bookTicker pair is required.
/// Errors are returned raw — the caller (`apply`) adds the single
/// "全量行情刷新失败" prefix — except the premium/book pair, which
/// keeps its own prefix (the old code doubled it through `apply`,
/// and this preserves that exact text).
/// The fundingInfo / ticker24hr failure log lines the old
/// MarketCatalog method wrote are gone: the venue has no log
/// handle. The keep-old-intervals / empty-volumes behavior is
/// unchanged.
pub(crate) async fn fetch_asterdex(http: &reqwest::Client) -> Result<Vec<MarketInfo>, String> {
    let specs_stale = {
        let at = SPECS_AT.lock().unwrap_or_else(|e| e.into_inner());
        match *at {
            None => true,
            Some(t) => t.elapsed() > ASTER_SPECS_TTL,
        }
    };
    if specs_stale {
        let v = get_json(http, &format!("{ASTER_REST_URL}/fapi/v3/exchangeInfo")).await?;
        let specs = aster_specs_from(&v);
        *SPECS.lock().unwrap_or_else(|e| e.into_inner()) = Some(specs);
        *SPECS_AT.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
    }
    let specs = SPECS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_default();
    let premium_url = format!("{ASTER_REST_URL}/fapi/v3/premiumIndex");
    let book_url = format!("{ASTER_REST_URL}/fapi/v3/ticker/bookTicker");
    let funding_info_url = format!("{ASTER_REST_URL}/fapi/v3/fundingInfo");
    let ticker_url = format!("{ASTER_REST_URL}/fapi/v3/ticker/24hr");
    let (premium, book, funding_info, ticker24) = tokio::join!(
        get_json(http, &premium_url),
        get_json(http, &book_url),
        get_json(http, &funding_info_url),
        get_json(http, &ticker_url),
    );
    // fundingInfo refreshes the cached interval map; on failure the
    // previous map is kept (intervals change rarely), and a symbol
    // never seen in it stays unknown.
    if let Ok(v) = funding_info {
        if let Some(list) = v.as_array() {
            let mut intervals: HashMap<String, f64> = HashMap::new();
            for f in list {
                if let (Some(s), Some(h)) =
                    (str_field(f, "symbol"), num_field(f, "fundingIntervalHours"))
                {
                    intervals.insert(s.to_string(), h);
                }
            }
            *INTERVALS.lock().unwrap_or_else(|e| e.into_inner()) = Some(intervals);
        }
    }
    let volumes = match ticker24 {
        Ok(v) => parse_asterdex_volumes(&v),
        Err(_) => HashMap::new(),
    };
    let intervals = INTERVALS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_default();
    match (premium, book) {
        (Ok(p), Ok(b)) => parse_asterdex(&specs, &p, &b, &intervals, &volumes, now_ms()),
        (Err(e), _) | (_, Err(e)) => Err(format!("asterdex 全量行情刷新失败：{e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(s: &str) -> serde_json::Value {
        serde_json::from_str(s).unwrap()
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

    /// PR-0.5: the V3 public responses parse exactly like the V1 ones.
    /// Fixtures are verbatim excerpts (BTCUSDT + ETHUSDT) of the real
    /// `/fapi/v3/*` responses fetched 2026-11-27 (no key). Full live
    /// counts that day: exchangeInfo 647 symbols (586 TRADING
    /// PERPETUAL), premiumIndex / fundingInfo 801, bookTicker 586,
    /// ticker/24hr 641.
    #[test]
    fn parse_asterdex_v3_live_fixtures() {
        let ei = json(
            r#"{
 "symbols": [
  {
   "symbol": "BTCUSDT",
   "filters": [
    {
     "minPrice": "1",
     "maxPrice": "1000000",
     "filterType": "PRICE_FILTER",
     "tickSize": "0.1"
    },
    {
     "stepSize": "0.001",
     "filterType": "LOT_SIZE",
     "maxQty": "1000",
     "minQty": "0.001"
    },
    {
     "stepSize": "0.001",
     "filterType": "MARKET_LOT_SIZE",
     "maxQty": "120",
     "minQty": "0.001"
    },
    {
     "limit": 200,
     "filterType": "MAX_NUM_ORDERS"
    },
    {
     "limit": 10,
     "filterType": "MAX_NUM_ALGO_ORDERS"
    },
    {
     "notional": "5",
     "filterType": "MIN_NOTIONAL"
    },
    {
     "multiplierDown": "0.9800",
     "multiplierUp": "1.0200",
     "ltMultiplierDown": "0",
     "multiplierDecimal": "4",
     "filterType": "PERCENT_PRICE",
     "ltMultiplierUp": "0"
    }
   ],
   "orderTypes": [
    "LIMIT",
    "MARKET",
    "STOP",
    "STOP_MARKET",
    "TAKE_PROFIT",
    "TAKE_PROFIT_MARKET",
    "TRAILING_STOP_MARKET"
   ],
   "timeInForce": [
    "GTC",
    "IOC",
    "GTX",
    "HIDDEN"
   ],
   "createTime": 1630045468108,
   "pair": "BTCUSDT",
   "contractType": "PERPETUAL",
   "deliveryDate": 4133404800000,
   "onboardDate": 1627628400000,
   "status": "TRADING",
   "maintMarginPercent": "2.5000",
   "requiredMarginPercent": "5.0000",
   "baseAsset": "BTC",
   "quoteAsset": "USDT",
   "marginAsset": "USDT",
   "pricePrecision": 1,
   "quantityPrecision": 3,
   "baseAssetPrecision": 8,
   "quotePrecision": 8,
   "underlyingType": "COIN",
   "underlyingSubType": [
    "Top"
   ],
   "symbolType": 0,
   "tradingMode": 0,
   "name": "",
   "channel": "",
   "sequenceNo": 10,
   "twapMinNotional": "1000",
   "imn": "16000.00",
   "tags": [],
   "settlePlan": 0,
   "triggerProtect": "0.0200",
   "liquidationFee": "0.025000",
   "marketTakeBound": "0.02"
  },
  {
   "symbol": "ETHUSDT",
   "filters": [
    {
     "minPrice": "0.01",
     "maxPrice": "1000000",
     "filterType": "PRICE_FILTER",
     "tickSize": "0.01"
    },
    {
     "stepSize": "0.001",
     "filterType": "LOT_SIZE",
     "maxQty": "10000",
     "minQty": "0.001"
    },
    {
     "stepSize": "0.001",
     "filterType": "MARKET_LOT_SIZE",
     "maxQty": "2000",
     "minQty": "0.001"
    },
    {
     "limit": 200,
     "filterType": "MAX_NUM_ORDERS"
    },
    {
     "limit": 10,
     "filterType": "MAX_NUM_ALGO_ORDERS"
    },
    {
     "notional": "5",
     "filterType": "MIN_NOTIONAL"
    },
    {
     "multiplierDown": "0.9800",
     "multiplierUp": "1.0200",
     "ltMultiplierDown": "0",
     "multiplierDecimal": "4",
     "filterType": "PERCENT_PRICE",
     "ltMultiplierUp": "0"
    }
   ],
   "orderTypes": [
    "LIMIT",
    "MARKET",
    "STOP",
    "STOP_MARKET",
    "TAKE_PROFIT",
    "TAKE_PROFIT_MARKET",
    "TRAILING_STOP_MARKET"
   ],
   "timeInForce": [
    "GTC",
    "IOC",
    "GTX",
    "HIDDEN"
   ],
   "createTime": 1630984677055,
   "pair": "ETHUSDT",
   "contractType": "PERPETUAL",
   "deliveryDate": 4133404800000,
   "onboardDate": 1630911600000,
   "status": "TRADING",
   "maintMarginPercent": "2.5000",
   "requiredMarginPercent": "5.0000",
   "baseAsset": "ETH",
   "quoteAsset": "USDT",
   "marginAsset": "USDT",
   "pricePrecision": 2,
   "quantityPrecision": 3,
   "baseAssetPrecision": 8,
   "quotePrecision": 8,
   "underlyingType": "COIN",
   "underlyingSubType": [
    "Top"
   ],
   "symbolType": 0,
   "tradingMode": 0,
   "name": "",
   "channel": "",
   "sequenceNo": 9,
   "twapMinNotional": "1000",
   "imn": "8000.00",
   "tags": [],
   "settlePlan": 0,
   "triggerProtect": "0.0200",
   "liquidationFee": "0.025000",
   "marketTakeBound": "0.02"
  }
 ]
}"#,
        );
        let specs = aster_specs_from(&ei);
        assert_eq!(specs.len(), 2);
        let premium = json(
            r#"[
 {
  "symbol": "ETHUSDT",
  "markPrice": "1692.99165892",
  "indexPrice": "1693.82162791",
  "estimatedSettlePrice": "1693.65756537",
  "lastFundingRate": "0.00004624",
  "interestRate": "0.00010000",
  "nextFundingTime": 1795737600000,
  "time": 1795716270000
 },
 {
  "symbol": "BTCUSDT",
  "markPrice": "60506.07349638",
  "indexPrice": "60524.37478261",
  "estimatedSettlePrice": "60524.94703762",
  "lastFundingRate": "0.00008069",
  "interestRate": "0.00010000",
  "nextFundingTime": 1795737600000,
  "time": 1795716270000
 }
]"#,
        );
        let book = json(
            r#"[
 {
  "symbol": "BTCUSDT",
  "bidPrice": "60510.1",
  "bidQty": "3.045",
  "askPrice": "60511.3",
  "askQty": "0.050",
  "time": 1795716271300,
  "lastUpdateId": 677007383825
 },
 {
  "symbol": "ETHUSDT",
  "bidPrice": "1692.92",
  "bidQty": "51.936",
  "askPrice": "1692.93",
  "askQty": "0.031",
  "time": 1795716271300,
  "lastUpdateId": 677007383813
 }
]"#,
        );
        let funding = json(
            r#"[
 {
  "symbol": "ETHUSDT",
  "interestRate": "0.00010000",
  "time": 1795716239000,
  "fundingIntervalHours": 8,
  "fundingFeeCap": 0.003,
  "fundingFeeFloor": -0.003
 },
 {
  "symbol": "BTCUSDT",
  "interestRate": "0.00010000",
  "time": 1795716239000,
  "fundingIntervalHours": 8,
  "fundingFeeCap": 0.003,
  "fundingFeeFloor": -0.003
 }
]"#,
        );
        let mut intervals = HashMap::new();
        for f in funding.as_array().unwrap() {
            intervals.insert(
                str_field(f, "symbol").unwrap().to_string(),
                num_field(f, "fundingIntervalHours").unwrap(),
            );
        }
        let ticker = json(
            r#"[
 {
  "symbol": "BTCUSDT",
  "priceChange": "846.0",
  "priceChangePercent": "1.418",
  "weightedAvgPrice": "60541.6",
  "lastPrice": "60504.7",
  "lastQty": "0.081",
  "openPrice": "59658.7",
  "highPrice": "61613.3",
  "lowPrice": "59460.0",
  "volume": "9326.913",
  "quoteVolume": "564666297.99",
  "openTime": 1795629840000,
  "closeTime": 1795716260250,
  "firstId": 150662245,
  "lastId": 150700705,
  "count": 38457
 },
 {
  "symbol": "ETHUSDT",
  "priceChange": "14.17",
  "priceChangePercent": "0.844",
  "weightedAvgPrice": "1693.89",
  "lastPrice": "1692.92",
  "lastQty": "1.429",
  "openPrice": "1678.75",
  "highPrice": "1729.52",
  "lowPrice": "1667.11",
  "volume": "183496.907",
  "quoteVolume": "310823420.03",
  "openTime": 1795629840000,
  "closeTime": 1795716269550,
  "firstId": 88221417,
  "lastId": 88246202,
  "count": 24783
 }
]"#,
        );
        let volumes = parse_asterdex_volumes(&ticker);
        let rows = parse_asterdex(
            &specs,
            &premium,
            &book,
            &intervals,
            &volumes,
            1_795_716_271_300,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        // The live premiumIndex order is not sorted by symbol.
        let btc = rows.iter().find(|r| r.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.symbol, "BTCUSDT");
        assert_eq!(btc.base_symbol, "BTC");
        assert_eq!(btc.settlement_currency, "USDT");
        assert_eq!(btc.tick_size, Some(0.1));
        assert_eq!(btc.step_size, Some(0.001));
        assert_eq!(btc.min_notional, Some(5.0));
        assert_eq!(btc.mark_px, Some(60506.07349638));
        assert_eq!(btc.bid_px, Some(60510.1));
        assert_eq!(btc.ask_px, Some(60511.3));
        assert!((btc.mid_px.unwrap() - 60510.7).abs() < 1e-9);
        assert_eq!(btc.funding_interval_hours, Some(8.0));
        assert!((btc.funding_1h.unwrap() - 0.00008069 / 8.0).abs() < 1e-12);
        assert_eq!(btc.volume_24h_usd, Some(564666297.99));
        // V3 has no openInterest endpoint: always None, never faked.
        assert_eq!(btc.open_interest_usd, None);
        assert_eq!(btc.max_leverage, None);
        let eth = rows.iter().find(|r| r.symbol == "ETHUSDT").unwrap();
        assert_eq!(eth.symbol, "ETHUSDT");
        assert_eq!(eth.tick_size, Some(0.01));
        assert_eq!(eth.mark_px, Some(1692.99165892));
        assert_eq!(eth.bid_px, Some(1692.92));
        assert_eq!(eth.ask_px, Some(1692.93));
        assert!((eth.funding_1h.unwrap() - 0.00004624 / 8.0).abs() < 1e-12);
        assert_eq!(eth.volume_24h_usd, Some(310823420.03));
        assert_eq!(eth.open_interest_usd, None);
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
}
