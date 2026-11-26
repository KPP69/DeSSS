//! Small shared helpers with no better home: a millisecond clock,
//! a lenient JSON-number reader, and the account live-status rule
//! (REST health only). Pure functions, no venue in particular.

/// Account live status from REST health only (there is no market WS on
/// an account anymore — account sync is REST polling, and public market
/// data comes from the on-demand catalog):
/// - REST ok → "connected" (latency from the last REST round-trip)
/// - REST down → "error" when the last failure is known, else "connecting"
pub fn live_status(rest_ok: bool, has_error: bool) -> &'static str {
    if rest_ok {
        "connected"
    } else if has_error {
        "error"
    } else {
        "connecting"
    }
}

/// Current time in milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// A JSON number-or-numeric-string as f64; anything else reads as 0.
pub(crate) fn f64_from(v: &serde_json::Value) -> f64 {
    match v {
        serde_json::Value::String(s) => s.parse().unwrap_or(0.0),
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_status_from_rest_only() {
        assert_eq!(live_status(true, false), "connected");
        assert_eq!(live_status(true, true), "connected");
        assert_eq!(live_status(false, true), "error");
        assert_eq!(live_status(false, false), "connecting");
    }
}
