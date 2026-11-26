//! Arcus catalog (PR-1.3 commit 2): /v1/markets + /v1/bbo.
//! Moved verbatim from `adapters/market_catalog.rs`.

use super::ARCUS_REST_URL;
use crate::core::market::{funding_apr_pct, get_json, num_field, str_field, MarketInfo};
use crate::core::symbol::normalize_base_symbol;

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
            // Arcus ticks are tiered (tickTiers; 82/87 markets carry 6
            // tiers) — only the base tier is used here. Tiered ticks
            // are NOT implemented in this phase (PR-1.3 known gap).
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

/// Best bid/ask from an Arcus `GET /v1/bbo/{market}` response.
fn parse_arcus_bbo(v: &serde_json::Value) -> (Option<f64>, Option<f64>) {
    (
        v.get("bestBid").and_then(|b| num_field(b, "price")),
        v.get("bestAsk").and_then(|a| num_field(a, "price")),
    )
}

pub(crate) async fn fetch_arcus_bbo(
    http: &reqwest::Client,
    symbol: &str,
) -> Result<(Option<f64>, Option<f64>), String> {
    let v = get_json(http, &format!("{ARCUS_REST_URL}/v1/bbo/{symbol}")).await?;
    Ok(parse_arcus_bbo(&v))
}

pub(crate) async fn fetch_arcus(http: &reqwest::Client) -> Result<Vec<MarketInfo>, String> {
    let v = get_json(http, &format!("{ARCUS_REST_URL}/v1/markets")).await?;
    parse_arcus(&v, crate::adapters::now_ms())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(s: &str) -> serde_json::Value {
        serde_json::from_str(s).unwrap()
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
}
