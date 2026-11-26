//! Lighter / Lighter RH stream: the `order_book` dial plan and frame
//! parser. Moved verbatim from `data_engine/realtime.rs` (PR-1.3
//! commit 1); the plan needs the numeric market id from the catalog,
//! so a missing id is a transient error, not a plan.

use crate::core::stream_kind::StreamKind;
use crate::venues::lighter::LighterNetwork;
use crate::venues::StreamPlan;

/// The `order_book` dial plan for one symbol. The market id comes
/// from the catalog; `None` means the catalog has not loaded this
/// symbol yet — a transient failure the engine retries.
pub fn stream_plan(
    symbol: &str,
    network: LighterNetwork,
    market_id: Option<u64>,
) -> Result<StreamPlan, String> {
    let market_id = market_id.ok_or_else(|| format!("目录中没有 {symbol} 的市场编号"))?;
    let base = match network {
        LighterNetwork::Mainnet => "wss://mainnet.zklighter.elliot.ai/stream",
        LighterNetwork::Robinhood => "wss://api.rh.lighter.xyz/stream",
    };
    Ok(StreamPlan {
        kind: StreamKind::Lighter,
        // `?readonly=true` is the documented way to read public data
        // from a restricted region.
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

/// One parsed Lighter `order_book` frame.
pub(crate) enum LighterFrame {
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
pub(crate) fn parse_lighter_book_frame(v: &serde_json::Value) -> Option<LighterFrame> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn val(s: &str) -> serde_json::Value {
        serde_json::from_str(s).expect("fixture")
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
    fn stream_plan_needs_the_catalog_market_id() {
        let plan = stream_plan("BTC", LighterNetwork::Mainnet, Some(1)).expect("plan");
        assert_eq!(
            plan.url,
            "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"
        );
        let sub: serde_json::Value =
            serde_json::from_str(plan.subscribe.as_ref().expect("subscribe")).expect("json");
        assert_eq!(sub["channel"].as_str().unwrap(), "order_book/1");
        let rh = stream_plan("BTC", LighterNetwork::Robinhood, Some(2)).expect("plan");
        assert_eq!(rh.url, "wss://api.rh.lighter.xyz/stream?readonly=true");
        let err = stream_plan("BTC", LighterNetwork::Mainnet, None).expect_err("no market id");
        assert_eq!(err, "目录中没有 BTC 的市场编号");
    }
}
