//! Arcus stream: the `l2Orderbook` dial plan and frame parser.
//! Moved verbatim from `data_engine/realtime.rs` (PR-1.3 commit 2).

use crate::core::stream_kind::StreamKind;
use crate::venues::StreamPlan;

/// The `l2Orderbook` dial plan for one symbol (20 levels).
pub fn stream_plan(symbol: &str) -> StreamPlan {
    StreamPlan {
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
    }
}

/// Arcus `l2Orderbook` frame: `contents.bids/asks` are `[price, size]`
/// string pairs; every frame is a full snapshot.
pub(crate) fn parse_arcus_l2(v: &serde_json::Value) -> Option<(Vec<(f64, f64)>, Vec<(f64, f64)>)> {
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
    fn arcus_l2_snapshot_parses() {
        let v = val(
            r#"{"type":"channel_data","channel":"l2Orderbook","id":"BTC-USD","contents":{"bids":[["64822.9","0.85"]],"asks":[["64826.9","0.85"]],"lastSequenceId":1,"globalSequenceId":2,"timestamp":1783750906942677}}"#,
        );
        let (bids, asks) = parse_arcus_l2(&v).expect("book");
        assert_eq!(bids, vec![(64822.9, 0.85)]);
        assert_eq!(asks, vec![(64826.9, 0.85)]);
    }

    #[test]
    fn stream_plan_subscribes_l2_orderbook() {
        let plan = stream_plan("BTC-USD");
        assert_eq!(plan.url, "wss://api.arcus.xyz/v1/ws");
        let sub: serde_json::Value =
            serde_json::from_str(plan.subscribe.as_ref().expect("subscribe")).expect("json");
        assert_eq!(sub["channel"].as_str().unwrap(), "l2Orderbook");
        assert_eq!(sub["id"].as_str().unwrap(), "BTC-USD");
        assert_eq!(sub["nLevels"].as_i64().unwrap(), 20);
    }
}
