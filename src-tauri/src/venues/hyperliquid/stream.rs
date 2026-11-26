//! Hyperliquid / Entropy stream: the `l2Book` dial plan and frame
//! parser. The catalog symbol goes out verbatim as the coin
//! (`io:NVDA` for Entropy — the `io` universe names already carry
//! the prefix). Moved verbatim from `data_engine/realtime.rs`
//! (PR-1.2).

use crate::core::stream_kind::StreamKind;
use crate::venues::StreamPlan;

/// The `l2Book` dial plan for one symbol. No dex branch: the
/// Hyperliquid and Entropy entries share this plan exactly.
pub fn stream_plan(symbol: &str) -> StreamPlan {
    StreamPlan {
        kind: StreamKind::Hyperliquid,
        url: "wss://api.hyperliquid.xyz/ws".to_string(),
        subscribe: Some(
            serde_json::json!({
                "method": "subscribe",
                "subscription": { "type": "l2Book", "coin": symbol }
            })
            .to_string(),
        ),
    }
}

/// Hyperliquid `l2Book` frame: `data.levels` is `[bids, asks]`, each
/// level `{"px", "sz", "n"}`. Returns `(bids, asks)` as float pairs.
pub fn parse_hl_l2book(v: &serde_json::Value) -> Option<(Vec<(f64, f64)>, Vec<(f64, f64)>)> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::venues::hyperliquid::{ENTROPY, HYPERLIQUID};
    use crate::venues::Venue;

    #[test]
    fn stream_subscribes_the_catalog_symbol_verbatim() {
        // Acceptance (PR-1.2): ("entropy", "io:NVDA") subscribes to
        // the coin "io:NVDA" — no second `io:` prefix.
        for (venue, symbol, want) in [
            (&HYPERLIQUID, "BTC", "BTC"),
            (&ENTROPY, "io:NVDA", "io:NVDA"),
        ] {
            let plan = venue.stream(symbol, None).expect("plan");
            assert_eq!(plan.kind, StreamKind::Hyperliquid);
            let sub: serde_json::Value =
                serde_json::from_str(plan.subscribe.as_ref().expect("subscribe")).expect("json");
            assert_eq!(
                sub["subscription"]["coin"].as_str().unwrap(),
                want,
                "{} {symbol}",
                venue.id()
            );
        }
    }
}
