//! The symbol rule (v3 §2.2): the catalog's `symbol`
//! passes through the whole chain unchanged. This helper
//! derives only the cross-exchange *search key* from a native
//! symbol — it never rewrites the canonical `symbol` itself.

/// Normalize an exchange-native symbol to a cross-exchange base name:
/// strip the `io:` prefix (Entropy), split off a `/QUOTE` or `-QUOTE`
/// suffix (Lighter `NVDA/USDG`, Arcus `BTC-USD`), strip a leading `rh`
/// (Lighter `rhSPY/USDC`), strip a trailing quote suffix (AsterDEX
/// `BTCUSDT`). `1000BONK`-style prefixes and FX pairs (`USDCAD`) are kept
/// as-is — they are distinct listings.
pub fn normalize_base_symbol(symbol: &str) -> String {
    let mut s = symbol.trim().to_uppercase();
    if let Some(rest) = s.strip_prefix("IO:") {
        s = rest.to_string();
    }
    // "NVDA/USDG" -> "NVDA"; "rhSPY/USDC" -> "SPY".
    let mut base = if let Some((head, _)) = s.split_once('/') {
        let mut head = head.to_string();
        if head.len() > 2 && head.starts_with("RH") {
            head = head[2..].to_string();
        }
        head
    } else {
        s.clone()
    };
    if let Some((head, _)) = base.split_once('-') {
        base = head.to_string();
    }
    for suffix in ["USDT", "USDC", "USDG", "USD"] {
        if base.len() > suffix.len() && base.ends_with(suffix) {
            base.truncate(base.len() - suffix.len());
            break;
        }
    }
    base
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_base_symbol_cases() {
        assert_eq!(normalize_base_symbol("BTC"), "BTC");
        assert_eq!(normalize_base_symbol("io:NVDA"), "NVDA");
        assert_eq!(normalize_base_symbol("BTCUSDT"), "BTC");
        assert_eq!(normalize_base_symbol("ETHUSDC"), "ETH");
        assert_eq!(normalize_base_symbol("BTC-USD"), "BTC");
        assert_eq!(normalize_base_symbol("NVDA/USDG"), "NVDA");
        assert_eq!(normalize_base_symbol("rhSPY/USDC"), "SPY");
        assert_eq!(normalize_base_symbol("1000BONK"), "1000BONK");
        assert_eq!(normalize_base_symbol("USDCAD"), "USDCAD");
        assert_eq!(normalize_base_symbol("kPEPEUSDT"), "KPEPE");
        assert_eq!(normalize_base_symbol("SPY"), "SPY");
        assert_eq!(normalize_base_symbol("BTC/USD"), "BTC");
    }
}
