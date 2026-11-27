//! Lighter / Lighter RH catalog (PR-1.3 commit 1): bulk market rows
//! from `orderBookDetails` + `funding-rates`, and the per-symbol BBO
//! fill from `orderBookOrders`. Moved verbatim from
//! `adapters/market_catalog.rs`.

use std::collections::HashMap;

use crate::core::market::{
    funding_1h_from, funding_apr_pct, get_json, next_hour_utc_ms, num_field, opt_f64, str_field,
    MarketInfo,
};
use crate::core::symbol::normalize_base_symbol;
use crate::venues::lighter::LighterNetwork;

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
            // No bulk bid/ask in REST, and last_trade_price is not a
            // mid: mid stays None until the BBO fill sets
            // (bid + ask) / 2 (see Lighter enrich).
            mid_px: None,
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

pub(crate) async fn fetch_lighter(
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
        crate::core::util::now_ms(),
    )
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

pub(crate) async fn fetch_lighter_book_orders(
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

#[cfg(test)]
mod tests {
    use super::*;

    fn json(s: &str) -> serde_json::Value {
        serde_json::from_str(s).unwrap()
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
        assert_eq!(btc.mid_px, None);
        // 8h-equivalent rate 0.000088 -> 1h 0.000011.
        assert!((btc.funding_1h.unwrap() - 0.000011).abs() < 1e-12);
        assert_eq!(btc.funding_interval_hours, Some(1.0));
        assert_eq!(btc.next_funding_time, Some(1_795_626_000_000));
        assert_eq!(btc.volume_24h_usd, Some(913793455.94));
        assert_eq!(btc.open_interest_usd, Some(224951137.406376));
    }
}
