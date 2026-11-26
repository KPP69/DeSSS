//! Hyperliquid / Entropy catalog: the bulk `metaAndAssetCtxs` fetch
//! and the per-hit `l2Book` BBO fill. Moved verbatim from
//! `adapters/market_catalog.rs` (PR-1.2).

use super::HL_INFO_URL;
use crate::adapters::market_catalog::{
    funding_apr_pct, hyperliquid_tick_size, next_hour_utc_ms, num_field, str_field, MarketInfo,
};
use crate::core::symbol::normalize_base_symbol;

pub async fn fetch_hyperliquid(
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

pub async fn fetch_l2book(
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

#[cfg(test)]
mod tests {
    use super::*;

    fn json(s: &str) -> serde_json::Value {
        serde_json::from_str(s).unwrap()
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
}
