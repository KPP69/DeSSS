//! AsterDEX stream: the `bookTicker` dial plan and frame parser.
//! Moved verbatim from `data_engine/realtime.rs` (PR-1.3 commit 2).
//! bookTicker is the top of book only — exactly what the Data page
//! shows — and needs no sequence handling.

use crate::core::stream_kind::StreamKind;
use crate::venues::StreamPlan;

/// The `bookTicker` dial plan for one symbol. The stream is encoded
/// in the URL itself, so there is no subscribe message.
pub fn stream_plan(symbol: &str) -> StreamPlan {
    StreamPlan {
        kind: StreamKind::Asterdex,
        url: format!(
            "wss://fstream.asterdex.com/ws/{}@bookTicker",
            symbol.to_lowercase()
        ),
        subscribe: None,
    }
}

/// AsterDEX `bookTicker` frame: top of book only.
pub(crate) fn parse_asterdex_book_ticker(v: &serde_json::Value) -> Option<(f64, f64, f64, f64)> {
    if v.get("e").and_then(|e| e.as_str()) != Some("bookTicker") {
        return None;
    }
    let f = |k: &str| v.get(k).and_then(|x| x.as_str())?.parse::<f64>().ok();
    Some((f("b")?, f("B")?, f("a")?, f("A")?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn val(s: &str) -> serde_json::Value {
        serde_json::from_str(s).expect("fixture")
    }

    #[test]
    fn asterdex_book_ticker_is_top_of_book() {
        let v = val(
            r#"{"e":"bookTicker","E":1,"u":1,"s":"BTCUSDT","b":"90000","B":"1.5","a":"90010","A":"0.5"}"#,
        );
        assert_eq!(
            parse_asterdex_book_ticker(&v),
            Some((90000.0, 1.5, 90010.0, 0.5))
        );
        let other = val(r#"{"e":"depthUpdate","s":"BTCUSDT"}"#);
        assert!(parse_asterdex_book_ticker(&other).is_none());
    }

    #[test]
    fn stream_plan_encodes_the_symbol_in_the_url() {
        let plan = stream_plan("BTCUSDT");
        assert_eq!(plan.url, "wss://fstream.asterdex.com/ws/btcusdt@bookTicker");
        assert!(plan.subscribe.is_none());
    }
}
